#!/usr/bin/env python3
"""
Fabric MCP Server
=================
MCP server exposing Microsoft Fabric Lakehouse tools for the SAS VA → Power BI
migration pipeline.

Tools
-----
authenticate          Acquire tokens for Fabric Items API + OneLake storage.
list_workspaces       List all Fabric workspaces the principal can access.
get_workspace         Get a single workspace by id or name.
list_lakehouses       List Lakehouse items in a workspace.
get_or_create_lakehouse  Get an existing Lakehouse or create a new one.
list_tables           List Delta tables registered in a Lakehouse.
upload_parquet        Upload a local Parquet file to OneLake (Files/ area).
register_delta_table  Register an uploaded folder as a Delta table (Tables/).
delete_file           Delete a file or folder from OneLake.
get_lakehouse         Get Lakehouse properties including SQL connection string.
run_notebook          Create & execute a one-shot Fabric Notebook.
get_item_status       Poll a long-running Fabric operation by operationId.

Environment variables
---------------------
FABRIC_TENANT_ID       Azure AD tenant id
FABRIC_CLIENT_ID       App registration client id
FABRIC_CLIENT_SECRET   Client secret (omit → device-code flow)
FABRIC_WORKSPACE_ID    Default workspace id (optional, can pass per-call)
"""

import json
import os
import time
from pathlib import Path

import httpx
import msal
from mcp.server.fastmcp import FastMCP

# ── Constants ─────────────────────────────────────────────────────────────────

FABRIC_API   = "https://api.fabric.microsoft.com/v1"
FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
STORAGE_SCOPE = "https://storage.azure.com/.default"
ONELAKE_HOST  = "onelake.dfs.fabric.microsoft.com"   # global endpoint

# ── Module-level auth state ────────────────────────────────────────────────────

_msal_app: msal.ClientApplication | None = None
_fabric_token: str | None = None
_storage_token: str | None = None

mcp = FastMCP("fabric")

# ── Auth helpers ──────────────────────────────────────────────────────────────

def _build_msal_app() -> msal.ClientApplication:
    """Create or return the cached MSAL application."""
    global _msal_app
    if _msal_app is not None:
        return _msal_app

    tenant_id  = os.environ["FABRIC_TENANT_ID"]
    client_id  = os.environ["FABRIC_CLIENT_ID"]
    secret     = os.environ.get("FABRIC_CLIENT_SECRET")
    authority  = f"https://login.microsoftonline.com/{tenant_id}"

    if secret:
        _msal_app = msal.ConfidentialClientApplication(
            client_id,
            authority=authority,
            client_credential=secret,
        )
    else:
        _msal_app = msal.PublicClientApplication(
            client_id,
            authority=authority,
        )
    return _msal_app


def _acquire_token(scope: str, force_refresh: bool = False) -> str:
    """
    Acquire a token for *scope*.
    - ConfidentialClientApplication  → client_credentials grant (silent)
    - PublicClientApplication        → device_code flow (interactive once,
                                       then token cache is reused)
    """
    app = _build_msal_app()

    # Try cache first (handles refresh automatically)
    if not force_refresh:
        if isinstance(app, msal.ConfidentialClientApplication):
            result = app.acquire_token_for_client(scopes=[scope])
        else:
            accounts = app.get_accounts()
            result = app.acquire_token_silent([scope], account=accounts[0]) if accounts else None

        if result and "access_token" in result:
            return result["access_token"]

    # Interactive device-code flow (PublicClientApplication only)
    if isinstance(app, msal.PublicClientApplication):
        flow = app.initiate_device_flow(scopes=[scope])
        if "user_code" not in flow:
            raise RuntimeError(f"Device flow initiation failed: {flow}")
        # Print to stderr so MCP stdout stays clean
        import sys
        print(flow["message"], file=sys.stderr, flush=True)
        result = app.acquire_token_by_device_flow(flow)
    else:
        result = app.acquire_token_for_client(scopes=[scope])

    if "access_token" not in result:
        raise RuntimeError(
            f"Token acquisition failed for scope={scope}: "
            f"{result.get('error')} — {result.get('error_description')}"
        )
    return result["access_token"]


def _fabric_headers(extra: dict | None = None) -> dict:
    global _fabric_token
    _fabric_token = _acquire_token(FABRIC_SCOPE)
    h = {
        "Authorization": f"Bearer {_fabric_token}",
        "Content-Type": "application/json",
    }
    if extra:
        h.update(extra)
    return h


def _storage_headers(extra: dict | None = None) -> dict:
    global _storage_token
    _storage_token = _acquire_token(STORAGE_SCOPE)
    h = {
        "Authorization": f"Bearer {_storage_token}",
        "x-ms-version": "2023-08-03",
    }
    if extra:
        h.update(extra)
    return h


def _fabric_get(path: str) -> dict:
    url = f"{FABRIC_API}/{path.lstrip('/')}"
    with httpx.Client(timeout=30) as client:
        r = client.get(url, headers=_fabric_headers())
    r.raise_for_status()
    return r.json()


def _fabric_post(path: str, body: dict) -> httpx.Response:
    url = f"{FABRIC_API}/{path.lstrip('/')}"
    with httpx.Client(timeout=60) as client:
        r = client.post(url, headers=_fabric_headers(), json=body)
    r.raise_for_status()
    return r


def _fabric_delete(path: str) -> httpx.Response:
    url = f"{FABRIC_API}/{path.lstrip('/')}"
    with httpx.Client(timeout=30) as client:
        r = client.delete(url, headers=_fabric_headers())
    r.raise_for_status()
    return r


# ── OneLake ADLS Gen2 helpers ─────────────────────────────────────────────────

def _onelake_url(workspace_id: str, lakehouse_id: str, path: str) -> str:
    """
    Build a DFS Gen2 URL for OneLake.
    Container = workspace_id, virtual directory = lakehouse_id/path
    """
    path = path.lstrip("/")
    return f"https://{ONELAKE_HOST}/{workspace_id}/{lakehouse_id}/{path}"


def _onelake_put_file(workspace_id: str, lakehouse_id: str,
                      remote_path: str, local_path: str) -> dict:
    """
    Upload a local file to OneLake using ADLS Gen2 create+append+flush.
    remote_path is relative to the Lakehouse root, e.g. "Files/data/sales.parquet"
    """
    data = Path(local_path).read_bytes()
    size = len(data)
    url  = _onelake_url(workspace_id, lakehouse_id, remote_path)

    with httpx.Client(timeout=120) as client:
        # 1. Create the file resource
        r = client.put(url, headers=_storage_headers({"x-ms-blob-type": ""}),
                       params={"resource": "file"})
        if r.status_code not in (201, 200):
            return {"error": f"create failed: {r.status_code} {r.text}"}

        # 2. Append the data
        r = client.patch(url,
                         headers=_storage_headers({"Content-Type": "application/octet-stream"}),
                         params={"action": "append", "position": "0"},
                         content=data)
        if r.status_code not in (202, 200):
            return {"error": f"append failed: {r.status_code} {r.text}"}

        # 3. Flush
        r = client.patch(url,
                         headers=_storage_headers(),
                         params={"action": "flush", "position": str(size)})
        if r.status_code not in (200, 202):
            return {"error": f"flush failed: {r.status_code} {r.text}"}

    return {"status": "ok", "remote_path": remote_path, "bytes": size}


# ── Tools ─────────────────────────────────────────────────────────────────────

@mcp.tool()
def authenticate() -> str:
    """
    Acquire and cache access tokens for the Fabric Items API and OneLake storage.
    Must be called before any other tool.
    If FABRIC_CLIENT_SECRET is not set, initiates an interactive device-code flow
    (the user must visit https://microsoft.com/devicelogin).
    Returns a JSON summary: {"fabric": "ok", "storage": "ok"}.
    """
    result = {}
    try:
        _acquire_token(FABRIC_SCOPE)
        result["fabric"] = "ok"
    except Exception as e:
        result["fabric"] = f"error: {e}"

    try:
        _acquire_token(STORAGE_SCOPE)
        result["storage"] = "ok"
    except Exception as e:
        result["storage"] = f"error: {e}"

    return json.dumps(result)


@mcp.tool()
def list_workspaces() -> str:
    """
    List all Fabric workspaces the authenticated principal can access.
    Returns JSON: [{"id": "...", "displayName": "...", "type": "..."}]
    """
    data = _fabric_get("workspaces")
    workspaces = [
        {"id": w["id"], "displayName": w["displayName"], "type": w.get("type", "")}
        for w in data.get("value", [])
    ]
    return json.dumps(workspaces)


@mcp.tool()
def get_workspace(workspace_id: str) -> str:
    """
    Get details of a specific workspace by its id.
    Returns JSON with id, displayName, type, capacityId, etc.
    """
    data = _fabric_get(f"workspaces/{workspace_id}")
    return json.dumps(data)


@mcp.tool()
def list_lakehouses(workspace_id: str) -> str:
    """
    List all Lakehouse items in a workspace.
    Returns JSON: [{"id": "...", "displayName": "...", "description": "..."}]
    """
    data = _fabric_get(f"workspaces/{workspace_id}/lakehouses")
    lakehouses = [
        {
            "id": item["id"],
            "displayName": item["displayName"],
            "description": item.get("description", ""),
        }
        for item in data.get("value", [])
    ]
    return json.dumps(lakehouses)


@mcp.tool()
def get_or_create_lakehouse(workspace_id: str, display_name: str,
                             description: str = "") -> str:
    """
    Return an existing Lakehouse by display_name, or create a new one.
    Returns JSON: {"id": "...", "displayName": "...", "created": true/false}
    """
    # Check if it already exists
    data = _fabric_get(f"workspaces/{workspace_id}/lakehouses")
    for item in data.get("value", []):
        if item["displayName"].lower() == display_name.lower():
            return json.dumps({
                "id": item["id"],
                "displayName": item["displayName"],
                "created": False,
            })

    # Create
    r = _fabric_post(f"workspaces/{workspace_id}/lakehouses", {
        "displayName": display_name,
        "description": description,
    })

    if r.status_code == 202:
        # Long-running operation — poll for result
        op_url = r.headers.get("Location") or r.headers.get("location")
        lakehouse = _poll_lro(op_url)
    else:
        lakehouse = r.json()

    return json.dumps({
        "id": lakehouse.get("id", ""),
        "displayName": lakehouse.get("displayName", display_name),
        "created": True,
    })


@mcp.tool()
def get_lakehouse(workspace_id: str, lakehouse_id: str) -> str:
    """
    Get full Lakehouse properties including SQL analytics endpoint connection string.
    Returns raw JSON from the Fabric Items API.
    """
    data = _fabric_get(f"workspaces/{workspace_id}/lakehouses/{lakehouse_id}")
    return json.dumps(data)


@mcp.tool()
def list_tables(workspace_id: str, lakehouse_id: str) -> str:
    """
    List Delta tables registered in the Lakehouse Tables/ section.
    Returns JSON: [{"name": "...", "type": "Managed/External", "location": "..."}]
    """
    data = _fabric_get(
        f"workspaces/{workspace_id}/lakehouses/{lakehouse_id}/tables"
    )
    tables = [
        {
            "name": t.get("name", ""),
            "type": t.get("type", ""),
            "location": t.get("location", ""),
            "format": t.get("format", ""),
        }
        for t in data.get("data", [])
    ]
    return json.dumps(tables)


@mcp.tool()
def upload_parquet(workspace_id: str, lakehouse_id: str,
                   local_path: str, remote_path: str) -> str:
    """
    Upload a local Parquet file to OneLake via the ADLS Gen2 API.

    remote_path is relative to the Lakehouse root and should start with
    "Files/" for unregistered files or "Tables/<table_name>/" for Delta tables.

    Example: remote_path="Files/synthetic/sales.parquet"

    Returns JSON: {"status": "ok", "remote_path": "...", "bytes": N}
    """
    if not Path(local_path).exists():
        return json.dumps({"error": f"local file not found: {local_path}"})
    result = _onelake_put_file(workspace_id, lakehouse_id, remote_path, local_path)
    return json.dumps(result)


@mcp.tool()
def register_delta_table(workspace_id: str, lakehouse_id: str,
                          table_name: str) -> str:
    """
    Register a folder under Tables/<table_name>/ as a Delta Lake table so it
    appears in the Lakehouse SQL analytics endpoint.

    The Parquet files must already be uploaded to
    Tables/<table_name>/ via upload_parquet before calling this.

    Returns JSON: {"status": "ok"|"error", "operationId": "...", "message": "..."}
    """
    try:
        r = _fabric_post(
            f"workspaces/{workspace_id}/lakehouses/{lakehouse_id}/tables/"
            f"{table_name}/load",
            {"relativePath": f"Tables/{table_name}", "pathType": "Folder"},
        )
        if r.status_code in (200, 202):
            op_id = r.headers.get("x-ms-operation-id", "")
            return json.dumps({"status": "ok", "operationId": op_id,
                                "message": f"Table {table_name} load triggered."})
        return json.dumps({"status": "error", "message": r.text})
    except httpx.HTTPStatusError as e:
        return json.dumps({"status": "error", "message": str(e)})


@mcp.tool()
def delete_file(workspace_id: str, lakehouse_id: str, remote_path: str) -> str:
    """
    Delete a file or folder from OneLake.
    remote_path is relative to the Lakehouse root, e.g. "Files/data/old.parquet".
    Returns JSON: {"status": "ok"|"error"}
    """
    url = _onelake_url(workspace_id, lakehouse_id, remote_path)
    token = _acquire_token(STORAGE_SCOPE)
    with httpx.Client(timeout=30) as client:
        r = client.delete(url, headers={
            "Authorization": f"Bearer {token}",
            "x-ms-version": "2023-08-03",
        })
    if r.status_code in (200, 202, 204):
        return json.dumps({"status": "ok"})
    return json.dumps({"status": "error", "code": r.status_code, "detail": r.text})


@mcp.tool()
def run_notebook(workspace_id: str, display_name: str,
                 lakehouse_id: str, pyspark_code: str) -> str:
    """
    Create a Fabric Notebook with the given PySpark code, attach it to the
    specified Lakehouse as the default Lakehouse, then trigger a run.

    Useful for post-load transformations (e.g. CONVERT TO DELTA, OPTIMIZE,
    schema evolution, or data quality checks).

    Returns JSON: {"notebookId": "...", "runId": "...", "status": "Submitted"}
    NOTE: Notebook runs are async. Use get_item_status to poll for completion.
    """
    # Build a minimal Fabric Notebook definition (ipynb base64-encoded)
    import base64

    notebook_json = {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {
            "language_info": {"name": "python"},
            "kernelspec": {"display_name": "PySpark", "language": "python",
                           "name": "synapse_pyspark"},
        },
        "cells": [
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": pyspark_code,
            }
        ],
    }
    ipynb_b64 = base64.b64encode(
        json.dumps(notebook_json).encode()
    ).decode()

    # Fabric Notebook definition payload
    definition_payload = {
        "displayName": display_name,
        "type": "Notebook",
        "definition": {
            "format": "ipynb",
            "parts": [
                {
                    "path": "notebook-content.ipynb",
                    "payload": ipynb_b64,
                    "payloadType": "InlineBase64",
                }
            ],
        },
        "properties": {
            "defaultLakehouseId": lakehouse_id,
            "defaultLakehouseWorkspaceId": workspace_id,
        },
    }

    try:
        r = _fabric_post(f"workspaces/{workspace_id}/notebooks", definition_payload)
    except httpx.HTTPStatusError as e:
        return json.dumps({"status": "error", "message": str(e)})

    if r.status_code == 202:
        notebook = _poll_lro(r.headers.get("Location") or r.headers.get("location"))
    else:
        notebook = r.json()

    notebook_id = notebook.get("id", "")
    if not notebook_id:
        return json.dumps({"status": "error", "message": "Notebook created but ID missing",
                            "raw": notebook})

    # Trigger a run
    try:
        run_r = _fabric_post(
            f"workspaces/{workspace_id}/notebooks/{notebook_id}/jobs/instances",
            {"executionData": {}},
        )
    except httpx.HTTPStatusError as e:
        return json.dumps({"notebookId": notebook_id, "status": "error",
                            "runError": str(e)})

    run_id = run_r.headers.get("x-ms-operation-id", "")
    return json.dumps({
        "notebookId": notebook_id,
        "runId": run_id,
        "status": "Submitted",
    })


@mcp.tool()
def get_item_status(workspace_id: str, item_id: str, job_instance_id: str) -> str:
    """
    Poll the status of a Fabric long-running job (notebook run, table load, etc.).
    Returns JSON: {"status": "Running"|"Succeeded"|"Failed", "error": "..."}
    """
    try:
        data = _fabric_get(
            f"workspaces/{workspace_id}/items/{item_id}"
            f"/jobs/instances/{job_instance_id}"
        )
        return json.dumps({
            "status": data.get("status", "Unknown"),
            "startTimeUtc": data.get("startTimeUtc"),
            "endTimeUtc": data.get("endTimeUtc"),
            "failureReason": data.get("failureReason"),
        })
    except httpx.HTTPStatusError as e:
        return json.dumps({"status": "error", "message": str(e)})


# ── LRO polling helper ─────────────────────────────────────────────────────────

def _poll_lro(operation_url: str, max_wait: int = 120) -> dict:
    """
    Poll a Fabric long-running operation URL until it completes or times out.
    Returns the final resource JSON.
    """
    if not operation_url:
        return {}
    deadline = time.time() + max_wait
    with httpx.Client(timeout=30) as client:
        while time.time() < deadline:
            r = client.get(operation_url, headers=_fabric_headers())
            if r.status_code == 200:
                body = r.json()
                state = body.get("status", body.get("state", ""))
                if state.lower() in ("succeeded", "completed", ""):
                    # Try to follow resourceLocation if present
                    resource_url = body.get("resourceLocation")
                    if resource_url:
                        rr = client.get(resource_url, headers=_fabric_headers())
                        if rr.status_code == 200:
                            return rr.json()
                    return body
                if state.lower() in ("failed", "canceled"):
                    raise RuntimeError(
                        f"LRO failed: {body.get('error', body)}"
                    )
            time.sleep(3)
    raise TimeoutError(f"LRO did not complete within {max_wait}s: {operation_url}")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    mcp.run()
