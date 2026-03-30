"""
Integration tests for the Fabric MCP server.

Requires environment variables:
  FABRIC_TENANT_ID, FABRIC_CLIENT_ID, FABRIC_CLIENT_SECRET, FABRIC_WORKSPACE_ID

Run with:
  pytest fabric_mcp/test_server.py -v
"""

import importlib.util
import json
import os
import tempfile
import time

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

# ── Load server module ─────────────────────────────────────────────────────────

_spec = importlib.util.spec_from_file_location(
    "fabric_server",
    os.path.join(os.path.dirname(__file__), "server.py"),
)
_m = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_m)

# ── Fixtures ───────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def workspace_id():
    wid = os.environ.get("FABRIC_WORKSPACE_ID")
    if not wid:
        pytest.skip("FABRIC_WORKSPACE_ID not set")
    return wid


@pytest.fixture(scope="session", autouse=True)
def auth():
    """Authenticate once for the entire test session."""
    result = json.loads(_m.authenticate())
    assert result.get("fabric") == "ok", f"Fabric auth failed: {result}"
    assert result.get("storage") == "ok", f"Storage auth failed: {result}"


@pytest.fixture(scope="session")
def test_lakehouse_id(workspace_id):
    """Return the ID of lakehouse_testcases (the write-safe test target)."""
    raw = json.loads(_m.list_lakehouses(workspace_id))
    for lh in raw:
        if lh["displayName"] == "lakehouse_testcases":
            return lh["id"]
    pytest.skip("lakehouse_testcases not found in workspace")


@pytest.fixture
def tmp_parquet(tmp_path):
    """Create a minimal Parquet file and return its path."""
    table = pa.table({"id": [1, 2, 3], "value": ["a", "b", "c"]})
    path = str(tmp_path / "test_upload.parquet")
    pq.write_table(table, path)
    return path


# ── authenticate ──────────────────────────────────────────────────────────────

class TestAuthenticate:
    def test_returns_ok_for_both_scopes(self):
        result = json.loads(_m.authenticate())
        assert result["fabric"] == "ok"
        assert result["storage"] == "ok"


# ── list_workspaces ───────────────────────────────────────────────────────────

class TestListWorkspaces:
    def test_returns_list(self):
        result = json.loads(_m.list_workspaces())
        assert isinstance(result, list)
        assert len(result) > 0

    def test_each_entry_has_required_fields(self):
        result = json.loads(_m.list_workspaces())
        for ws in result:
            assert "id" in ws
            assert "displayName" in ws
            assert "type" in ws

    def test_known_workspace_present(self, workspace_id):
        result = json.loads(_m.list_workspaces())
        ids = [ws["id"] for ws in result]
        assert workspace_id in ids


# ── get_workspace ─────────────────────────────────────────────────────────────

class TestGetWorkspace:
    def test_returns_workspace_details(self, workspace_id):
        result = json.loads(_m.get_workspace(workspace_id))
        assert result["id"] == workspace_id
        assert "displayName" in result
        assert "capacityRegion" in result

    def test_invalid_id_raises(self):
        with pytest.raises(Exception):
            _m.get_workspace("00000000-0000-0000-0000-000000000000")


# ── list_lakehouses ───────────────────────────────────────────────────────────

class TestListLakehouses:
    def test_returns_list(self, workspace_id):
        result = json.loads(_m.list_lakehouses(workspace_id))
        assert isinstance(result, list)
        assert len(result) > 0

    def test_each_entry_has_required_fields(self, workspace_id):
        result = json.loads(_m.list_lakehouses(workspace_id))
        for lh in result:
            assert "id" in lh
            assert "displayName" in lh

    def test_known_lakehouses_present(self, workspace_id):
        result = json.loads(_m.list_lakehouses(workspace_id))
        names = [lh["displayName"] for lh in result]
        assert "lh_testcases" in names
        assert "lakehouse_testcases" in names


# ── get_lakehouse ─────────────────────────────────────────────────────────────

class TestGetLakehouse:
    def test_returns_sql_endpoint(self, workspace_id, test_lakehouse_id):
        result = json.loads(_m.get_lakehouse(workspace_id, test_lakehouse_id))
        assert result["id"] == test_lakehouse_id
        props = result.get("properties", {})
        sql_ep = props.get("sqlEndpointProperties", {})
        assert sql_ep.get("connectionString"), "SQL endpoint connection string missing"
        assert sql_ep.get("provisioningStatus") == "Success"

    def test_onelake_paths_present(self, workspace_id, test_lakehouse_id):
        result = json.loads(_m.get_lakehouse(workspace_id, test_lakehouse_id))
        props = result.get("properties", {})
        assert "oneLakeTablesPath" in props
        assert "oneLakeFilesPath" in props


# ── list_tables ───────────────────────────────────────────────────────────────

class TestListTables:
    def test_returns_list(self, workspace_id, test_lakehouse_id):
        result = json.loads(_m.list_tables(workspace_id, test_lakehouse_id))
        assert isinstance(result, list)

    def test_each_entry_has_schema_name_type(self, workspace_id, test_lakehouse_id):
        result = json.loads(_m.list_tables(workspace_id, test_lakehouse_id))
        for t in result:
            assert "schema" in t
            assert "name" in t
            assert "type" in t

    def test_known_tables_present(self, workspace_id, test_lakehouse_id):
        result = json.loads(_m.list_tables(workspace_id, test_lakehouse_id))
        names = [t["name"] for t in result]
        # Tables we confirmed exist in lakehouse_testcases
        assert "vdsqf50_cdtcdk" in names

    def test_works_for_schema_enabled_lakehouse(self, workspace_id):
        # lh_testcases is also schema-enabled — must not raise
        lakehouses = json.loads(_m.list_lakehouses(workspace_id))
        lh = next((l for l in lakehouses if l["displayName"] == "lh_testcases"), None)
        if lh is None:
            pytest.skip("lh_testcases not found")
        result = json.loads(_m.list_tables(workspace_id, lh["id"]))
        assert isinstance(result, list)


# ── get_or_create_lakehouse ───────────────────────────────────────────────────

class TestGetOrCreateLakehouse:
    def test_returns_existing_without_creating(self, workspace_id):
        result = json.loads(_m.get_or_create_lakehouse(workspace_id, "lh_testcases"))
        assert result["displayName"] == "lh_testcases"
        assert result["created"] is False
        assert result["id"]

    def test_create_and_delete_new_lakehouse(self, workspace_id):
        name = "lh_pytest_tmp"
        # Ensure it doesn't exist first (clean up any leftover)
        _delete_lakehouse_if_exists(workspace_id, name)

        result = json.loads(_m.get_or_create_lakehouse(workspace_id, name,
                                                        description="pytest temp"))
        try:
            assert result["displayName"] == name
            assert result["created"] is True
            assert result["id"]
            # Idempotency: calling again should return created=False
            result2 = json.loads(_m.get_or_create_lakehouse(workspace_id, name))
            assert result2["created"] is False
            assert result2["id"] == result["id"]
        finally:
            _delete_lakehouse_if_exists(workspace_id, name)


# ── upload_parquet + delete_file ──────────────────────────────────────────────

class TestUploadParquet:
    REMOTE_PATH = "Files/pytest/test_upload.parquet"

    def test_upload_and_delete(self, workspace_id, test_lakehouse_id, tmp_parquet):
        # Upload
        result = json.loads(_m.upload_parquet(
            workspace_id, test_lakehouse_id,
            tmp_parquet, self.REMOTE_PATH,
        ))
        assert result.get("status") == "ok", f"Upload failed: {result}"
        assert result["bytes"] > 0
        assert result["remote_path"] == self.REMOTE_PATH

        # Delete
        del_result = json.loads(_m.delete_file(
            workspace_id, test_lakehouse_id, self.REMOTE_PATH,
        ))
        assert del_result.get("status") == "ok", f"Delete failed: {del_result}"

    def test_upload_missing_local_file_returns_error(self, workspace_id, test_lakehouse_id):
        result = json.loads(_m.upload_parquet(
            workspace_id, test_lakehouse_id,
            "/nonexistent/path/file.parquet",
            "Files/pytest/nope.parquet",
        ))
        assert "error" in result


# ── register_delta_table ──────────────────────────────────────────────────────

class TestWriteDeltaTable:
    SCHEMA = "dbo"
    TABLE = "pytest_delta_tmp"

    def test_write_overwrite(self, workspace_id, test_lakehouse_id, tmp_parquet):
        result = json.loads(_m.write_delta_table(
            workspace_id, test_lakehouse_id,
            self.SCHEMA, self.TABLE,
            tmp_parquet, mode="overwrite",
        ))
        assert result.get("status") == "ok", f"write failed: {result}"
        assert result["rows"] == 3
        assert result["delta_version"] == 0
        assert "uri" in result

    def test_write_append(self, workspace_id, test_lakehouse_id, tmp_parquet):
        # First write (overwrite to establish v0)
        _m.write_delta_table(workspace_id, test_lakehouse_id,
                             self.SCHEMA, self.TABLE, tmp_parquet, mode="overwrite")
        # Append a second batch
        result = json.loads(_m.write_delta_table(
            workspace_id, test_lakehouse_id,
            self.SCHEMA, self.TABLE,
            tmp_parquet, mode="append",
        ))
        assert result.get("status") == "ok", f"append failed: {result}"
        assert result["delta_version"] == 1

    def test_missing_local_file_returns_error(self, workspace_id, test_lakehouse_id):
        result = json.loads(_m.write_delta_table(
            workspace_id, test_lakehouse_id,
            self.SCHEMA, self.TABLE,
            "/nonexistent/path.parquet", mode="overwrite",
        ))
        assert "error" in result


# ── run_notebook + get_item_status ────────────────────────────────────────────

class TestRunNotebook:
    NB_NAME = "pytest_tmp_notebook"

    def test_run_simple_notebook(self, workspace_id, test_lakehouse_id):
        code = "print('pytest notebook run ok')"
        result = json.loads(_m.run_notebook(
            workspace_id=workspace_id,
            display_name=self.NB_NAME,
            lakehouse_id=test_lakehouse_id,
            pyspark_code=code,
        ))
        notebook_id = result.get("notebookId", "")
        try:
            assert notebook_id, f"run_notebook failed: {result}"
            assert result.get("status") == "Submitted"

            run_id = result.get("runId", "")
            # Poll status if we got a runId
            if run_id:
                status_result = json.loads(_m.get_item_status(
                    workspace_id, notebook_id, run_id,
                ))
                assert "status" in status_result
        finally:
            # Delete the notebook (best-effort cleanup)
            if notebook_id:
                try:
                    _m._fabric_delete(
                        f"workspaces/{workspace_id}/notebooks/{notebook_id}"
                    )
                except Exception:
                    pass


# ── get_item_status ───────────────────────────────────────────────────────────

class TestGetItemStatus:
    def test_invalid_ids_return_error(self, workspace_id):
        result = json.loads(_m.get_item_status(
            workspace_id,
            "00000000-0000-0000-0000-000000000000",
            "00000000-0000-0000-0000-000000000000",
        ))
        # Should return an error dict, not raise
        assert "status" in result or "error" in result


# ── Helpers ───────────────────────────────────────────────────────────────────

def _delete_lakehouse_if_exists(workspace_id: str, display_name: str) -> None:
    """Delete a lakehouse by display name if it exists (best-effort cleanup)."""
    import httpx
    lakehouses = json.loads(_m.list_lakehouses(workspace_id))
    for lh in lakehouses:
        if lh["displayName"] == display_name:
            try:
                _m._fabric_delete(
                    f"workspaces/{workspace_id}/lakehouses/{lh['id']}"
                )
            except httpx.HTTPStatusError:
                pass
            break
