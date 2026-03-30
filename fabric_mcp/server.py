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
delete_file           Delete a file or folder from OneLake.
get_lakehouse         Get Lakehouse properties including SQL connection string.
write_delta_table     Write a local Parquet as a Delta table via delta-rs.
run_notebook          Create & execute a one-shot Fabric Notebook.
get_item_status       Poll a long-running Fabric operation by operationId.
deploy_semantic_model  Deploy a local .SemanticModel folder to Fabric.
deploy_report          Deploy a local .Report folder to Fabric, linked to a semantic model.
refresh_semantic_model Trigger a refresh/framing of a Semantic Model (required after DirectLake deploy).
list_semantic_models   List all Semantic Models in a workspace.
list_reports           List all Reports in a workspace.

Environment variables
---------------------
FABRIC_TENANT_ID       Azure AD tenant id
FABRIC_CLIENT_ID       App registration client id
FABRIC_CLIENT_SECRET   Client secret (omit → device-code flow)
FABRIC_WORKSPACE_ID    Default workspace id (optional, can pass per-call)
"""

import base64
import json
import os
import struct
import time
from pathlib import Path

import httpx
import msal
import pyodbc
import pyarrow.parquet as pq
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.filedatalake import DataLakeServiceClient
from deltalake import write_deltalake, DeltaTable
from mcp.server.fastmcp import FastMCP

# ── Constants ─────────────────────────────────────────────────────────────────

FABRIC_API    = "https://api.fabric.microsoft.com/v1"
FABRIC_SCOPE  = "https://api.fabric.microsoft.com/.default"
STORAGE_SCOPE = "https://storage.azure.com/.default"
POWERBI_API   = "https://api.powerbi.com/v1.0/myorg"
POWERBI_SCOPE = "https://analysis.windows.net/powerbi/api/.default"
ONELAKE_HOST  = "onelake.dfs.fabric.microsoft.com"   # global endpoint

# ── Module-level auth state ────────────────────────────────────────────────────

_msal_app: msal.ClientApplication | None = None
_fabric_token: str | None = None
_storage_token: str | None = None
_powerbi_token: str | None = None

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


def _powerbi_headers(extra: dict | None = None) -> dict:
    global _powerbi_token
    _powerbi_token = _acquire_token(POWERBI_SCOPE)
    h = {
        "Authorization": f"Bearer {_powerbi_token}",
        "Content-Type": "application/json",
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


def _sql_connect(sql_server: str, database: str) -> pyodbc.Connection:
    """
    Open a pyodbc connection to a Fabric SQL analytics endpoint using
    an Azure AD access token (service principal or device-code flow).
    """
    token = _acquire_token("https://database.windows.net/.default")
    token_bytes = token.encode("utf-16-le")
    token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
    conn_str = (
        "Driver={ODBC Driver 18 for SQL Server};"
        f"Server={sql_server};"
        f"Database={database};"
        "Encrypt=yes;TrustServerCertificate=no;"
    )
    # SQL_COPT_SS_ACCESS_TOKEN = 1256
    return pyodbc.connect(conn_str, attrs_before={1256: token_struct})


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


# ── OneLake folder helpers ────────────────────────────────────────────────────

def _onelake_datalake_client(workspace_id: str) -> DataLakeServiceClient:
    """Return an ADLS Gen2 client pointed at the OneLake global endpoint."""
    from azure.core.credentials import AccessToken as AzureAccessToken

    class _MsalCredential:
        """Wraps _acquire_token so the Azure SDK can call get_token()."""
        def get_token(self, *scopes, **_kw):
            scope = scopes[0] if scopes else STORAGE_SCOPE
            token = _acquire_token(scope)
            return AzureAccessToken(token, int(time.time()) + 3600)

    return DataLakeServiceClient(
        account_url="https://onelake.dfs.fabric.microsoft.com",
        credential=_MsalCredential(),
    )


def _drop_onelake_table_folder(*, workspace_id: str, lakehouse_id: str,
                                schema: str, table_name: str) -> None:
    """
    Delete Tables/{schema}/{table_name} inside the Lakehouse on OneLake and
    wait until the path is confirmed gone (so delta-rs sees a clean slate).
    Re-creates the schema directory afterwards.
    Uses GUIDs (workspace_id / lakehouse_id) — display names are not supported
    on all workspaces (FriendlyNameSupportDisabled).
    """
    svc = _onelake_datalake_client(workspace_id)
    fs = svc.get_file_system_client(workspace_id)
    rel = f"{lakehouse_id}/Tables/{schema}/{table_name}"
    parent = f"{lakehouse_id}/Tables/{schema}"

    try:
        fs.delete_directory(rel, recursive=True)
    except ResourceNotFoundError:
        pass  # already gone
    except TypeError:
        # Some SDK versions pass 'recursive' twice — use DirectoryClient fallback
        try:
            fs.get_directory_client(rel).delete_directory()
        except ResourceNotFoundError:
            pass

    # Poll until confirmed deleted (delta-rs needs a clean path)
    for _ in range(30):
        try:
            fs.get_directory_client(rel).get_directory_properties()
            time.sleep(1.0)
        except ResourceNotFoundError:
            break

    # Re-create the schema directory so the parent exists
    try:
        fs.get_directory_client(parent).create_directory()
    except Exception:
        pass


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
    List tables registered in the Lakehouse via the SQL analytics endpoint.
    Works with both schema-enabled and classic Lakehouses.
    Returns JSON: [{"schema": "...", "name": "...", "type": "BASE TABLE|VIEW"}]
    """
    lh = json.loads(get_lakehouse(workspace_id, lakehouse_id))
    props = lh.get("properties", {})
    sql_ep = props.get("sqlEndpointProperties", {})
    sql_server = sql_ep.get("connectionString", "")
    display_name = lh.get("displayName", lakehouse_id)

    if not sql_server:
        return json.dumps({"error": "SQL analytics endpoint not provisioned"})

    try:
        conn = _sql_connect(sql_server, display_name)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE "
            "FROM INFORMATION_SCHEMA.TABLES "
            "ORDER BY TABLE_SCHEMA, TABLE_NAME"
        )
        tables = [
            {"schema": row[0], "name": row[1], "type": row[2]}
            for row in cursor.fetchall()
        ]
        conn.close()
    except Exception as e:
        return json.dumps({"error": str(e)})

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
def write_delta_table(workspace_id: str, lakehouse_id: str,
                      schema: str, table_name: str,
                      local_parquet_path: str,
                      mode: str = "overwrite") -> str:
    """
    Write a local Parquet file directly to a Fabric Lakehouse as a Delta table
    using delta-rs over the OneLake abfss:// endpoint.

    Works with schema-enabled lakehouses. Supports overwrite (default) and append.
    On overwrite, the existing table folder (including _delta_log) is deleted first
    so Delta sees a clean path.

    Args:
        workspace_id:       Fabric workspace GUID.
        lakehouse_id:       Lakehouse item GUID (from list_lakehouses).
        schema:             Target schema name (e.g. "dbo", "work").
        table_name:         Target table name.
        local_parquet_path: Absolute path to a local Parquet file.
        mode:               "overwrite" (default) or "append".

    Returns JSON: {"status": "ok", "uri": "...", "rows": N, "delta_version": N}
    """
    if not Path(local_parquet_path).exists():
        return json.dumps({"error": f"local file not found: {local_parquet_path}"})

    # Use GUIDs in the abfss URI — display names are not supported on all workspaces
    table_uri = (
        f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/"
        f"{lakehouse_id}/Tables/{schema}/{table_name}"
    )

    storage_token = _acquire_token(STORAGE_SCOPE)
    storage_options = {
        "azure_use_fabric_endpoint": "true",
        "bearer_token": storage_token,
    }

    try:
        pa_table = pq.read_table(local_parquet_path)

        # On overwrite: drop the table folder so delta-rs starts from a clean path
        if mode == "overwrite":
            _drop_onelake_table_folder(
                workspace_id=workspace_id,
                lakehouse_id=lakehouse_id,
                schema=schema,
                table_name=table_name,
            )

        write_deltalake(table_uri, pa_table, mode=mode, storage_options=storage_options)

        delta_version = None
        try:
            delta_version = DeltaTable(table_uri, storage_options=storage_options).version()
        except Exception:
            pass

        return json.dumps({
            "status": "ok",
            "uri": table_uri,
            "rows": pa_table.num_rows,
            "delta_version": delta_version,
        })
    except Exception as e:
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
                "source": pyspark_code.splitlines(keepends=True),
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
        # LRO response for notebook creation doesn't carry an id — look it up by name
        items = _fabric_get(f"workspaces/{workspace_id}/notebooks").get("value", [])
        for item in items:
            if item.get("displayName") == display_name:
                notebook_id = item["id"]
                break
    if not notebook_id:
        return json.dumps({"status": "error", "message": "Notebook created but ID could not be resolved",
                            "raw": notebook})

    # Trigger a run
    try:
        run_r = _fabric_post(
            f"workspaces/{workspace_id}/items/{notebook_id}/jobs/instances"
            "?jobType=RunNotebook",
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


# ── Power BI / Semantic Model helpers ────────────────────────────────────────

def _folder_to_parts(folder: Path, exclude: set[str] | None = None) -> list[dict]:
    """
    Recursively read every file under *folder* and return a Fabric API
    'parts' list: [{"path": relative_path, "payload": base64, "payloadType": "InlineBase64"}]

    Skips:
    - macOS artefacts (.DS_Store, __MACOSX)
    - .platform files (Git integration metadata; causes 400 when deploying via Items API)
    - any extra filenames passed in *exclude*
    """
    skip = {".DS_Store", ".platform"} | (exclude or set())
    parts = []
    for file_path in sorted(folder.rglob("*")):
        if not file_path.is_file():
            continue
        if file_path.name in skip or "__MACOSX" in file_path.parts:
            continue
        rel = file_path.relative_to(folder).as_posix()
        payload = base64.b64encode(file_path.read_bytes()).decode()
        parts.append({"path": rel, "payload": payload, "payloadType": "InlineBase64"})
    return parts


def _pbir_with_model_id(report_folder: Path, workspace_id: str,
                        semantic_model_id: str) -> list[dict]:
    """
    Build the parts list for a Report folder, replacing the datasetReference
    in definition.pbir so it points to *semantic_model_id* by Fabric connection.

    Fabric requires the GUID embedded as semanticModelId= (camelCase) in
    byConnection.connectionString.
    """
    pbir_path = report_folder / "definition.pbir"
    pbir = json.loads(pbir_path.read_text(encoding="utf-8"))

    pbir["$schema"] = (
        "https://developer.microsoft.com/json-schemas/fabric/item/report"
        "/definitionProperties/2.0.0/schema.json"
    )
    # Fabric requires the GUID embedded as the semanticModelId parameter
    # (camelCase) in the connectionString field.
    pbir["datasetReference"] = {
        "byConnection": {
            "connectionString": f"semanticModelId={semantic_model_id}",
        }
    }

    _skip = {".DS_Store", ".platform"}
    parts = []
    for file_path in sorted(report_folder.rglob("*")):
        if not file_path.is_file():
            continue
        if file_path.name in _skip or "__MACOSX" in file_path.parts:
            continue
        rel = file_path.relative_to(report_folder).as_posix()
        if rel == "definition.pbir":
            content = json.dumps(pbir, indent=2).encode()
        else:
            content = file_path.read_bytes()
        parts.append({
            "path": rel,
            "payload": base64.b64encode(content).decode(),
            "payloadType": "InlineBase64",
        })
    return parts


def _find_item_by_name(workspace_id: str, item_type: str, display_name: str) -> dict | None:
    """Return the first item matching display_name, or None."""
    data = _fabric_get(f"workspaces/{workspace_id}/{item_type}")
    for item in data.get("value", []):
        if item.get("displayName", "").lower() == display_name.lower():
            return item
    return None


# ── Tools: Semantic Model & Report ────────────────────────────────────────────

@mcp.tool()
def list_semantic_models(workspace_id: str) -> str:
    """
    List all Semantic Models in a workspace.
    Returns JSON: [{"id": "...", "displayName": "..."}]
    """
    data = _fabric_get(f"workspaces/{workspace_id}/semanticModels")
    return json.dumps([
        {"id": i["id"], "displayName": i["displayName"]}
        for i in data.get("value", [])
    ])


@mcp.tool()
def list_reports(workspace_id: str) -> str:
    """
    List all Reports in a workspace.
    Returns JSON: [{"id": "...", "displayName": "..."}]
    """
    data = _fabric_get(f"workspaces/{workspace_id}/reports")
    return json.dumps([
        {"id": i["id"], "displayName": i["displayName"]}
        for i in data.get("value", [])
    ])


@mcp.tool()
def deploy_semantic_model(workspace_id: str, display_name: str,
                           semantic_model_folder: str) -> str:
    """
    Deploy a local .SemanticModel folder to Fabric as a Semantic Model item.

    If an item with *display_name* already exists its definition is updated
    (updateDefinition); otherwise a new item is created.

    Args:
        workspace_id:            Fabric workspace GUID.
        display_name:            Name for the Semantic Model in the workspace.
        semantic_model_folder:   Absolute path to the .SemanticModel directory
                                 (must contain definition.pbism + definition/).

    Returns JSON: {"id": "...", "displayName": "...", "created": true|false}
    """
    folder = Path(semantic_model_folder)
    if not folder.is_dir():
        return json.dumps({"error": f"folder not found: {semantic_model_folder}"})

    parts = _folder_to_parts(folder)

    existing = _find_item_by_name(workspace_id, "semanticModels", display_name)
    if existing:
        # Update existing definition
        item_id = existing["id"]
        try:
            r = _fabric_post(
                f"workspaces/{workspace_id}/semanticModels/{item_id}/updateDefinition",
                {"definition": {"format": "TMDL", "parts": parts}},
            )
            if r.status_code == 202:
                _poll_lro(r.headers.get("Location") or r.headers.get("location"))
        except httpx.HTTPStatusError as e:
            return json.dumps({"error": str(e)})
        return json.dumps({"id": item_id, "displayName": display_name, "created": False})

    # Create new
    try:
        r = _fabric_post(
            f"workspaces/{workspace_id}/semanticModels",
            {"displayName": display_name,
             "definition": {"format": "TMDL", "parts": parts}},
        )
    except httpx.HTTPStatusError as e:
        return json.dumps({"error": str(e)})

    if r.status_code == 202:
        result = _poll_lro(r.headers.get("Location") or r.headers.get("location"))
    else:
        result = r.json()

    item_id = result.get("id", "")
    if not item_id:
        # Resolve by name (same pattern as run_notebook)
        found = _find_item_by_name(workspace_id, "semanticModels", display_name)
        item_id = found["id"] if found else ""

    return json.dumps({"id": item_id, "displayName": display_name, "created": True})


@mcp.tool()
def deploy_report(workspace_id: str, display_name: str,
                  report_folder: str, semantic_model_id: str) -> str:
    """
    Deploy a local .Report folder to Fabric as a Report item.

    The definition.pbir datasetReference is rewritten to point to
    *semantic_model_id* by Fabric connection (byConnection), so the local
    byPath reference is replaced automatically.

    If an item with *display_name* already exists its definition is updated;
    otherwise a new item is created.

    Args:
        workspace_id:       Fabric workspace GUID.
        display_name:       Name for the Report in the workspace.
        report_folder:      Absolute path to the .Report directory
                            (must contain definition.pbir + definition/).
        semantic_model_id:  ID of the deployed Semantic Model (from deploy_semantic_model).

    Returns JSON: {"id": "...", "displayName": "...", "created": true|false}
    """
    folder = Path(report_folder)
    if not folder.is_dir():
        return json.dumps({"error": f"folder not found: {report_folder}"})

    parts = _pbir_with_model_id(folder, workspace_id, semantic_model_id)

    existing = _find_item_by_name(workspace_id, "reports", display_name)
    if existing:
        item_id = existing["id"]
        try:
            r = _fabric_post(
                f"workspaces/{workspace_id}/reports/{item_id}/updateDefinition",
                {"definition": {"format": "PBIR-Legacy", "parts": parts}},
            )
            if r.status_code == 202:
                _poll_lro(r.headers.get("Location") or r.headers.get("location"))
        except httpx.HTTPStatusError as e:
            return json.dumps({"error": str(e)})
        return json.dumps({"id": item_id, "displayName": display_name, "created": False})

    # Create new
    try:
        r = _fabric_post(
            f"workspaces/{workspace_id}/reports",
            {"displayName": display_name,
             "definition": {"format": "PBIR-Legacy", "parts": parts}},
        )
    except httpx.HTTPStatusError as e:
        return json.dumps({"error": str(e)})

    if r.status_code == 202:
        result = _poll_lro(r.headers.get("Location") or r.headers.get("location"))
    else:
        result = r.json()

    item_id = result.get("id", "")
    if not item_id:
        found = _find_item_by_name(workspace_id, "reports", display_name)
        item_id = found["id"] if found else ""

    return json.dumps({"id": item_id, "displayName": display_name, "created": True})


def _ensure_dataset_connection(workspace_id: str, dataset_id: str) -> None:
    """
    Ensure every SQL datasource in the dataset has an explicit Fabric connection
    bound with ServicePrincipal credentials.

    Required for Import mode models querying a Fabric SQL Analytics Endpoint:
    the Power BI service rejects refreshes on "default" (unbound) connections.

    Flow:
      1. TakeOver the dataset so this SP can manage it.
      2. Discover datasources via the Power BI API.
      3. For each SQL datasource that has no bound connection (no gatewayId):
         a. Find or create a matching Fabric ShareableCloud SQL connection.
         b. Bind the connection to the semantic model.
    """
    tenant_id  = os.environ.get("FABRIC_TENANT_ID", "")
    client_id  = os.environ.get("FABRIC_CLIENT_ID", "")
    client_secret = os.environ.get("FABRIC_CLIENT_SECRET", "")

    # 1. TakeOver
    takeover_url = (f"{POWERBI_API}/groups/{workspace_id}"
                    f"/datasets/{dataset_id}/Default.TakeOver")
    with httpx.Client(timeout=30) as client:
        client.post(takeover_url, headers=_powerbi_headers())

    # 2. Discover datasources
    ds_url = f"{POWERBI_API}/groups/{workspace_id}/datasets/{dataset_id}/datasources"
    with httpx.Client(timeout=30) as client:
        r = client.get(ds_url, headers=_powerbi_headers())
        r.raise_for_status()
    datasources = r.json().get("value", [])

    for ds in datasources:
        if ds.get("datasourceType", "").lower() != "sql":
            continue
        if ds.get("gatewayId"):
            continue   # already bound to a connection

        conn_details = ds.get("connectionDetails", {})
        sql_server = conn_details.get("server", "")
        sql_database = conn_details.get("database", "")
        if not sql_server:
            continue

        # 3a. Find existing connection or create one
        conn_path = f"{sql_server};{sql_database}".lower()
        with httpx.Client(timeout=30) as client:
            existing = client.get(f"{FABRIC_API}/connections", headers=_fabric_headers())
        conn_id = ""
        for c in existing.json().get("value", []):
            path = c.get("connectionDetails", {}).get("path", "").lower()
            if path == conn_path and c.get("connectivityType") == "ShareableCloud":
                conn_id = c["id"]
                break

        if not conn_id:
            create_body = {
                "connectivityType": "ShareableCloud",
                "displayName": f"sm_{dataset_id[:8]}_sql",
                "connectionDetails": {
                    "type": "SQL",
                    "creationMethod": "Sql",
                    "parameters": [
                        {"dataType": "Text", "name": "server",   "value": sql_server},
                        {"dataType": "Text", "name": "database", "value": sql_database},
                    ],
                },
                "privacyLevel": "Organizational",
                "credentialDetails": {
                    "singleSignOnType": "None",
                    "connectionEncryption": "Encrypted",
                    "credentials": {
                        "credentialType": "ServicePrincipal",
                        "servicePrincipalClientId": client_id,
                        "servicePrincipalSecret": client_secret,
                        "tenantId": tenant_id,
                    },
                },
            }
            with httpx.Client(timeout=30) as client:
                cr = client.post(f"{FABRIC_API}/connections",
                                 json=create_body, headers=_fabric_headers())
                cr.raise_for_status()
            conn_id = cr.json()["id"]

        # 3b. Bind connection to the semantic model
        bind_body = {
            "connectionBinding": {
                "id": conn_id,
                "connectivityType": "ShareableCloud",
                "connectionDetails": {
                    "type": "SQL",
                    "path": f"{sql_server};{sql_database}",
                },
            }
        }
        with httpx.Client(timeout=30) as client:
            client.post(
                f"{FABRIC_API}/workspaces/{workspace_id}"
                f"/semanticModels/{dataset_id}/bindConnection",
                json=bind_body,
                headers=_fabric_headers(),
            )


@mcp.tool()
def refresh_semantic_model(workspace_id: str, semantic_model_id: str) -> str:
    """
    Trigger a refresh (framing) of a Fabric Semantic Model.

    Required after deploying a DirectLake semantic model — Direct Lake tables
    start unprocessed and fall back to DirectQuery until the first framing
    operation completes.

    Also use after changing the underlying Lakehouse schema to re-frame the model.

    Args:
        workspace_id:       Fabric workspace GUID.
        semantic_model_id:  ID of the Semantic Model to refresh.

    Returns JSON: {"status": "ok"|"error", "operationId": "..."}
    """
    # For Import mode models: ensure a Fabric connection with SP credentials
    # is bound so the Power BI service can authenticate to the SQL endpoint.
    try:
        _ensure_dataset_connection(workspace_id, semantic_model_id)
    except Exception:
        pass  # Best-effort — refresh may still succeed if connection already bound

    # Fabric semantic model refresh uses the Power BI REST API (different base URL + scope)
    url = f"{POWERBI_API}/groups/{workspace_id}/datasets/{semantic_model_id}/refreshes"
    try:
        with httpx.Client(timeout=60) as client:
            r = client.post(url, json={}, headers=_powerbi_headers())
            r.raise_for_status()

        # Poll the refresh history until the latest entry completes (max 10 min).
        # Import mode refresh of a SQL Analytics Endpoint can take several minutes.
        history_url = url + "?$top=1"
        deadline = time.time() + 600
        with httpx.Client(timeout=30) as client:
            while time.time() < deadline:
                time.sleep(5)
                hr = client.get(history_url, headers=_powerbi_headers())
                hr.raise_for_status()
                entries = hr.json().get("value", [])
                if not entries:
                    continue
                entry = entries[0]
                state = entry.get("status", "")
                if state == "Completed":
                    return json.dumps({"status": "ok", "refreshId": entry.get("requestId", "")})
                if state in ("Failed", "Cancelled"):
                    return json.dumps({"status": "error", "message": state,
                                       "detail": entry.get("serviceExceptionJson", "")})
        return json.dumps({"status": "error", "message": "Refresh timed out after 5 minutes"})
    except httpx.HTTPStatusError as e:
        return json.dumps({"status": "error", "message": str(e)})


@mcp.tool()
def execute_dax_query(workspace_id: str, semantic_model_id: str, dax: str) -> str:
    """
    Execute a DAX query against a deployed Fabric Semantic Model.

    Uses the Power BI executeQueries REST API. Useful for verifying that a
    DirectLake model is properly framed and its tables are queryable.

    Args:
        workspace_id:       Fabric workspace GUID.
        semantic_model_id:  Semantic Model GUID.
        dax:                DAX query string, e.g. "EVALUATE ROW(\\"n\\", COUNTROWS(MyTable))"

    Returns JSON: {"status": "ok", "results": [...]} or {"status": "error", ...}
    """
    url = (f"{POWERBI_API}/groups/{workspace_id}"
           f"/datasets/{semantic_model_id}/executeQueries")
    body = {
        "queries": [{"query": dax}],
        "serializerSettings": {"includeNulls": True},
    }
    try:
        with httpx.Client(timeout=60) as client:
            r = client.post(url, json=body, headers=_powerbi_headers())
            r.raise_for_status()
        results = r.json().get("results", [])
        return json.dumps({"status": "ok", "results": results})
    except httpx.HTTPStatusError as e:
        return json.dumps({"status": "error", "message": str(e),
                           "detail": e.response.text})


@mcp.tool()
def export_report_page(workspace_id: str, report_id: str,
                       page_name: str, output_path: str,
                       export_format: str = "PNG") -> str:
    """
    Export a single report page to a file (PNG, PDF, or PPTX).

    Uses the Power BI Export-to-File API, which is asynchronous: the tool
    starts the export, polls until complete, then writes the binary to
    *output_path* on the local filesystem.

    Args:
        workspace_id:   Fabric workspace GUID.
        report_id:      Report item GUID.
        page_name:      Internal page name (e.g. "ReportSection").
        output_path:    Local path to write the exported file.
        export_format:  "PNG" (default), "PDF", or "PPTX".

    Returns JSON: {"status": "ok", "output_path": "...", "bytes": N}
    """
    url = f"{POWERBI_API}/groups/{workspace_id}/reports/{report_id}/ExportTo"
    body = {
        "format": export_format.upper(),
        "powerBIReportConfiguration": {
            "pages": [{"pageName": page_name}],
        },
    }
    try:
        with httpx.Client(timeout=60) as client:
            r = client.post(url, json=body, headers=_powerbi_headers())
            r.raise_for_status()
            export_id = r.json().get("id", "")
            if not export_id:
                return json.dumps({"status": "error", "message": "No export id returned",
                                   "detail": r.text})

            # Poll until Succeeded (or timeout after 5 min)
            poll_url = (f"{POWERBI_API}/groups/{workspace_id}/reports"
                        f"/{report_id}/exports/{export_id}")
            deadline = time.time() + 300
            while time.time() < deadline:
                time.sleep(3)
                pr = client.get(poll_url, headers=_powerbi_headers())
                pr.raise_for_status()
                status = pr.json().get("status", "")
                if status == "Succeeded":
                    break
                if status == "Failed":
                    return json.dumps({"status": "error",
                                       "message": "Export failed",
                                       "detail": pr.text})
            else:
                return json.dumps({"status": "error", "message": "Export timed out"})

            # Download the file
            file_url = poll_url + "/file"
            fr = client.get(file_url, headers=_powerbi_headers())
            fr.raise_for_status()

        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        Path(output_path).write_bytes(fr.content)
        return json.dumps({"status": "ok", "output_path": output_path,
                           "bytes": len(fr.content)})
    except httpx.HTTPStatusError as e:
        return json.dumps({"status": "error", "message": str(e),
                           "detail": e.response.text})


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
