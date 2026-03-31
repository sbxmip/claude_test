#!/usr/bin/env python3
"""
Agent 3a — Semantic Model Agent
=================================
Part of the SAS Visual Analytics → Power BI migration pipeline.

Reads canonical_model.json (Agent 2 output) and:
  1. Creates a Fabric Lakehouse
  2. Loads data per source table → Lakehouse Delta tables
       --use-real-data : downloads real parquet from SAS Viya CAS
       (default)       : Claude generates realistic synthetic test data
  3. Generates TMDL (Import mode via SQL Analytics Endpoint)
  4. Deploys Semantic Model to Fabric
  5. Refreshes + verifies with DAX queries
  6. Writes manifest.json → picked up by Agent 3b

Usage:
    python agent3a_semantic_model.py [--report-id UUID] [--input-dir PATH]
                                     [--use-real-data]

Environment:
    ANTHROPIC_API_KEY
    FABRIC_WORKSPACE_ID
    FABRIC_TENANT_ID, FABRIC_CLIENT_ID, FABRIC_CLIENT_SECRET
    # required only with --use-real-data:
    VIYA_BASE_URL      (default: https://harvai.westeurope.cloudapp.azure.com)
    VIYA_USERNAME
    VIYA_PASSWORD
"""

import argparse
import json
import os
import sys
import uuid
import tempfile
from pathlib import Path

import anthropic
import importlib.util
import pyarrow as pa
import pyarrow.parquet as pq

# ── Load Fabric MCP server ────────────────────────────────────────────────────

_spec = importlib.util.spec_from_file_location(
    "fabric_server",
    Path(__file__).parent / "fabric_mcp" / "server.py",
)
_m = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_m)

# ── Constants ─────────────────────────────────────────────────────────────────

MODEL       = "claude-opus-4-6"
MAX_TOKENS  = 16000
OUTPUT_BASE = "pbi"

VIYA_BASE_URL = os.environ.get("VIYA_BASE_URL",
                               "https://harvai.westeurope.cloudapp.azure.com")

# ── Viya / CAS helpers ────────────────────────────────────────────────────────

def _viya_token() -> str:
    """Obtain a fresh Viya bearer token via password grant."""
    import httpx as _httpx
    username = os.environ.get("VIYA_USERNAME", "")
    password = os.environ.get("VIYA_PASSWORD", "")
    if not username or not password:
        raise RuntimeError("Set VIYA_USERNAME and VIYA_PASSWORD for --use-real-data")
    r = _httpx.post(
        f"{VIYA_BASE_URL}/SASLogon/oauth/token",
        data={"grant_type": "password", "username": username, "password": password},
        headers={"Accept": "application/json",
                 "Content-Type": "application/x-www-form-urlencoded"},
        auth=("sas.ec", ""),
        timeout=30,
        verify=False,
    )
    r.raise_for_status()
    return r.json()["access_token"]


def _cas_download(server: str, library: str, table: str,
                  token: str, max_rows: int = 10_000) -> pa.Table:
    """
    Download an entire CAS table via the casProxy table.fetch action.
    Returns a PyArrow Table.  Paginates at 50 000 rows per fetch.
    """
    import httpx as _httpx

    headers = {"Authorization": f"Bearer {token}",
               "Accept": "application/json",
               "Content-Type": "application/json"}
    base = f"{VIYA_BASE_URL}/casProxy/servers/{server}/cas"

    # Create a CAS session
    r = _httpx.post(f"{base}/sessions", headers=headers,
                    json={}, timeout=30, verify=False)
    r.raise_for_status()
    session_id = r.json()["session"]
    print(f"      [CAS] session {session_id}")

    fetch_url = f"{base}/sessions/{session_id}/actions/table.fetch"
    page_size  = 1_000    # CAS table.fetch server-side max is 1000 rows/call
    all_rows:   list[list] = []
    col_names:  list[str]  = []
    col_types:  list[str]  = []
    row_from    = 1

    # Load the table into CAS memory (required before fetch if not already loaded)
    print(f"      [CAS] loading {library}.{table} into CAS memory…")
    load_url = f"{base}/sessions/{session_id}/actions/table.loadTable"
    lr = _httpx.post(load_url, headers=headers,
                     json={"path": table, "caslib": library,
                           "promote": False, "replace": True},
                     timeout=120, verify=False)
    # Ignore errors here — table might already be loaded, or load not needed
    if lr.status_code == 200:
        lj = lr.json()
        disp = lj.get("disposition", {})
        if disp.get("severity") not in ("Normal", "Warning", None, ""):
            print(f"      [CAS] loadTable warning: {disp.get('formattedStatus', '')}")
    else:
        print(f"      [CAS] loadTable returned {lr.status_code} — continuing")

    print(f"      [CAS] downloading {library}.{table}…")
    while True:
        body = {
            "table": {"name": table, "caslib": library},
            "from":  row_from,
            "to":    row_from + page_size - 1,
        }
        r = _httpx.post(fetch_url, headers=headers, json=body,
                        timeout=120, verify=False)
        r.raise_for_status()
        rj = r.json()
        if "results" not in rj:
            raise KeyError(f"no 'results' in CAS response: {json.dumps(rj)[:400]}")
        if "Fetch" not in rj["results"]:
            raise KeyError(f"no 'Fetch' in results: {json.dumps(rj['results'])[:400]}")
        fetch = rj["results"]["Fetch"]

        # Parse schema on first page
        if not col_names:
            for col in fetch["schema"]:
                if col["name"] == "_Index_":
                    continue            # skip CAS row index
                col_names.append(col["name"])
                col_types.append(col["type"])
            idx_col = next((i for i, c in enumerate(fetch["schema"])
                            if c["name"] == "_Index_"), None)

        rows = fetch["rows"]
        if not rows:
            break

        for raw in rows:
            # Drop _Index_ column if present
            if idx_col is not None:
                raw = raw[:idx_col] + raw[idx_col + 1:]
            all_rows.append(raw)

        print(f"      [CAS] fetched {len(all_rows)} rows")
        if len(rows) < page_size or len(all_rows) >= max_rows:
            break
        row_from += page_size

    # Delete the session (best-effort)
    try:
        _httpx.delete(f"{base}/sessions/{session_id}",
                      headers=headers, timeout=10, verify=False)
    except Exception:
        pass

    if not all_rows:
        raise RuntimeError(f"CAS table {library}.{table} returned no rows")

    # Build PyArrow table column by column
    _PA_MAP = {"double": pa.float64(), "int": pa.int64(),
               "string": pa.string(), "char": pa.string()}
    arrays = []
    for i, (cname, ctype) in enumerate(zip(col_names, col_types)):
        vals = [row[i] for row in all_rows]
        arr_type = _PA_MAP.get(ctype, pa.string())
        try:
            arrays.append(pa.array(vals, type=arr_type))
        except Exception:
            arrays.append(pa.array([str(v) if v is not None else None
                                    for v in vals], type=pa.string()))

    return pa.table(dict(zip(col_names, arrays)))

# ── Helpers ───────────────────────────────────────────────────────────────────

def make_id() -> str:
    return uuid.uuid4().hex[:20]


# ── Canonical model reader ────────────────────────────────────────────────────

def _read_canonical(canonical: dict, section: str,
                    name: str = None, page_id: str = None) -> str:
    sm = canonical.get("semantic_model", {})

    if section == "overview":
        return json.dumps({
            "report":      canonical.get("report", {}),
            "sources":     [{"name": s["name"], "label": s["label"]}
                            for s in sm.get("sources", [])],
            "dimensions":  [d["name"] for d in sm.get("dimensions", [])],
            "measures":    [m["name"] for m in sm.get("measures", [])],
            "metrics":     [m["name"] for m in sm.get("metrics", [])],
            "parameters":  sm.get("parameters", []),   # full detail: name, label, default, range
            "hierarchies": sm.get("hierarchies", []),  # full detail: name, label, source, levels[]
            "pages":       [{"id": p["id"],
                             "display_name": p["display_name"],
                             "visual_count": len(p.get("visuals", []))}
                            for p in canonical.get("pages", [])],
        })

    if section == "source":
        src = next((s for s in sm.get("sources", []) if s["name"] == name), None)
        if not src:
            return json.dumps({"error": f"source '{name}' not found"})
        dims = [d for d in sm.get("dimensions", []) if d.get("source") == name]
        msrs = [m for m in sm.get("measures",   []) if m.get("source") == name]
        return json.dumps({"source": src, "dimensions": dims, "measures": msrs})

    if section == "metrics":
        return json.dumps({
            "metrics":    sm.get("metrics", []),
            "parameters": sm.get("parameters", []),
        })

    if section == "page":
        page = next((p for p in canonical.get("pages", [])
                     if p["id"] == page_id), None)
        if not page:
            return json.dumps({"error": f"page '{page_id}' not found"})
        return json.dumps(page)

    if section == "filters":
        # Return all static filter definitions with resolved dimension columns
        # so the data generator knows exactly which values must appear in each column
        filter_defs = canonical.get("filters", [])
        dim_map = {d["name"]: d for d in sm.get("dimensions", [])}
        result = []
        for f in filter_defs:
            if f.get("type") == "static":
                dim = dim_map.get(f.get("dimension", ""), {})
                result.append({
                    "id":      f["id"],
                    "column":  dim.get("expr", f.get("dimension", "")),
                    "source":  dim.get("source", ""),
                    "operator": f.get("operator", "in"),
                    "values":  f.get("values", []),
                })
        return json.dumps(result)

    return json.dumps({"error": f"unknown section '{section}'"})


# ── Runtime state ─────────────────────────────────────────────────────────────

class _State:
    def __init__(self, canonical: dict, workspace_id: str,
                 output_dir: Path, source_report_id: str,
                 data_sources: list, use_real_data: bool = False):
        self.canonical        = canonical
        self.workspace_id     = workspace_id
        self.output_dir       = output_dir
        self.source_report_id = source_report_id
        self.report_name      = canonical.get("report", {}).get("name", "Report")
        self.safe_name        = self.report_name.replace(" ", "_")
        self.use_real_data    = use_real_data

        # ground truth from data_sources.json (list of source dicts)
        self.data_sources: list = data_sources

        # cached Viya token (real-data mode only)
        self._viya_token: str = ""

        # set by setup_lakehouse
        self.lakehouse_id:   str = ""
        self.lakehouse_name: str = ""
        self.sql_endpoint:   str = ""
        self.sql_database:   str = ""

        # accumulated by generate_test_data: source_name → table_name in Fabric
        self.loaded_tables:  dict = {}
        # source_name → frozenset of column names actually loaded
        self.loaded_columns: dict = {}

        # set by write_semantic_model
        self.sm_dir: Path | None = None

        # set by deploy_semantic_model
        self.sm_id: str = ""


def _sas_columns(data_sources: list, source_name: str) -> set:
    """Unique xref column names for a source (by name or label)."""
    for ds in data_sources:
        if ds["name"] == source_name or ds["label"] == source_name:
            return {c["xref"] for c in ds.get("columns", []) if c.get("xref")}
    return set()


# ── Tool executor ─────────────────────────────────────────────────────────────

_PA_TYPES = {
    "int64": pa.int64(),   "integer": pa.int64(),   "bigint": pa.int64(),
    "double": pa.float64(), "float": pa.float64(),  "decimal": pa.float64(),
    "string": pa.string(), "varchar": pa.string(),  "text": pa.string(),
    "boolean": pa.bool_(),
    "dateTime": pa.string(), "date": pa.string(), "datetime": pa.string(),
}


def _execute(name: str, inputs: dict, state: _State) -> str:
    short = {k: (str(v)[:80] + "…" if len(str(v)) > 80 else str(v))
             for k, v in inputs.items()}
    print(f"    → {name}({json.dumps(short, ensure_ascii=False)})")

    # ── read_canonical_model ──────────────────────────────────────────────────
    if name == "read_canonical_model":
        return _read_canonical(
            state.canonical,
            section=inputs["section"],
            name=inputs.get("name"),
            page_id=inputs.get("page_id"),
        )

    # ── setup_lakehouse ───────────────────────────────────────────────────────
    if name == "setup_lakehouse":
        import time as _time
        lh = json.loads(_m.get_or_create_lakehouse(
            workspace_id=state.workspace_id,
            display_name=inputs["display_name"],
        ))
        lh_id = lh["id"]

        # SQL endpoint may take up to ~30 s to provision after lakehouse creation
        conn = ""
        for _attempt in range(10):
            details = json.loads(_m.get_lakehouse(state.workspace_id, lh_id))
            props   = details.get("properties", {})
            sql_ep  = props.get("sqlEndpointProperties") or {}
            conn    = sql_ep.get("connectionString", "")
            if conn:
                break
            print(f"      [setup_lakehouse] SQL endpoint not ready yet, retrying in 10 s…")
            _time.sleep(10)

        if not conn:
            return json.dumps({"error": "SQL endpoint did not provision within 100 s — retry"})

        state.lakehouse_id   = lh_id
        state.lakehouse_name = inputs["display_name"]
        state.sql_endpoint   = conn
        state.sql_database   = inputs["display_name"]

        return json.dumps({
            "status":       "ok",
            "lakehouse_id": lh_id,
            "sql_endpoint": conn,
            "sql_database": inputs["display_name"],
            "created":      lh.get("created", False),
        })

    # ── generate_test_data ────────────────────────────────────────────────────
    if name == "generate_test_data":
        if not state.lakehouse_id:
            return json.dumps({"error": "call setup_lakehouse first"})

        source_name = inputs["source_name"]
        table_name  = inputs["table_name"]
        columns     = inputs["columns"]   # [{name, type}]
        rows        = inputs["rows"]      # list of dicts

        if not rows:
            return json.dumps({"error": "rows must not be empty"})

        # Build column arrays from rows
        col_arrays = {
            col["name"]: [row.get(col["name"]) for row in rows]
            for col in columns
        }
        arrow_table = pa.table(col_arrays)

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            tmp = f.name
        try:
            pq.write_table(arrow_table, tmp)
            result = json.loads(_m.write_delta_table(
                workspace_id      = state.workspace_id,
                lakehouse_id      = state.lakehouse_id,
                schema            = "dbo",
                table_name        = table_name,
                local_parquet_path = tmp,
                mode              = "overwrite",
            ))
        finally:
            Path(tmp).unlink(missing_ok=True)

        if result.get("status") == "ok":
            state.loaded_tables[source_name]  = table_name
            state.loaded_columns[source_name] = {c["name"] for c in columns}
            return json.dumps({
                "status":        "ok",
                "table_name":    table_name,
                "rows_loaded":   result["rows"],
                "delta_version": result["delta_version"],
            })
        return json.dumps(result)

    # ── download_cas_table ────────────────────────────────────────────────────
    if name == "download_cas_table":
        if not state.lakehouse_id:
            return json.dumps({"error": "call setup_lakehouse first"})

        source_name = inputs["source_name"]
        table_name  = inputs["table_name"]

        # Resolve canonical source → data_source entry
        # The canonical model uses internal names (e.g. "rand_retaildemo") while
        # data_sources.json uses SAS labels (e.g. "RAND_RETAILDEMO"). Also try
        # looking up the canonical source's label field for a match.
        canonical_src = next(
            (s for s in state.canonical.get("semantic_model", {}).get("sources", [])
             if s["name"] == source_name),
            None,
        )
        canonical_label = canonical_src.get("label", "") if canonical_src else ""
        src_lower = source_name.lower()
        lbl_lower = canonical_label.lower()
        cas_src = next(
            (s for s in state.data_sources
             if s["name"] == source_name
             or s["label"] == source_name
             or s["label"].lower() == src_lower
             or s["name"].lower() == src_lower
             or (lbl_lower and s["label"].lower() == lbl_lower)),
            None,
        )
        if not cas_src:
            return json.dumps({"error": f"source '{source_name}' not found in data_sources"})
        if cas_src.get("type") != "relational":
            return json.dumps({"error": f"source '{source_name}' is not relational (type={cas_src.get('type')})"})

        conn   = cas_src.get("connection", {})
        server  = conn.get("server", "cas-shared-default")
        library = conn.get("library", "")
        table   = conn.get("table", "")
        if not library or not table:
            return json.dumps({"error": f"missing library/table in connection for '{source_name}'"})

        # Get (or refresh) Viya token
        if not state._viya_token:
            try:
                state._viya_token = _viya_token()
                print(f"      [Viya] authenticated OK")
            except Exception as e:
                return json.dumps({"error": f"Viya auth failed: {e}"})

        # Download CAS table (default 10k rows — real structure + representative values)
        max_rows = int(inputs.get("max_rows", 10_000))
        try:
            arrow_table = _cas_download(server, library, table,
                                        state._viya_token, max_rows=max_rows)
        except Exception as e:
            # Token may have expired — retry once with fresh token
            print(f"      [CAS] download failed ({e}), refreshing token…")
            try:
                state._viya_token = _viya_token()
                arrow_table = _cas_download(server, library, table,
                                            state._viya_token, max_rows=max_rows)
            except Exception as e2:
                return json.dumps({"error": f"CAS download failed: {e2}"})

        # Upload to Fabric Lakehouse as Delta table
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            tmp = f.name
        try:
            pq.write_table(arrow_table, tmp)
            result = json.loads(_m.write_delta_table(
                workspace_id       = state.workspace_id,
                lakehouse_id       = state.lakehouse_id,
                schema             = "dbo",
                table_name         = table_name,
                local_parquet_path = tmp,
                mode               = "overwrite",
            ))
        finally:
            Path(tmp).unlink(missing_ok=True)

        if result.get("status") == "ok":
            col_names = {c["name"] for c in (inputs.get("columns") or [])} \
                        or set(arrow_table.schema.names)
            state.loaded_tables[source_name]  = table_name
            state.loaded_columns[source_name] = set(arrow_table.schema.names)
            return json.dumps({
                "status":        "ok",
                "table_name":    table_name,
                "rows_loaded":   result["rows"],
                "columns":       arrow_table.schema.names,
                "delta_version": result["delta_version"],
            })
        return json.dumps(result)

    # ── write_semantic_model ──────────────────────────────────────────────────
    if name == "write_semantic_model":
        import shutil as _shutil
        sm_dir = state.output_dir / f"{state.safe_name}.SemanticModel"
        if sm_dir.exists():
            _shutil.rmtree(sm_dir)   # wipe stale files from previous runs
        sm_dir.mkdir(parents=True, exist_ok=True)
        for rel, content in inputs["files"].items():
            fp = sm_dir / rel
            fp.parent.mkdir(parents=True, exist_ok=True)
            fp.write_text(
                json.dumps(content, indent=2) if isinstance(content, dict) else content,
                encoding="utf-8",
            )
        (sm_dir / ".platform").write_text(json.dumps({
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/platform/platformProperties.json",
            "version": "2.0",
            "config":   {"logicalId": str(uuid.uuid4())},
            "metadata": {"type": "SemanticModel", "displayName": state.safe_name},
        }, indent=2), encoding="utf-8")
        state.sm_dir = sm_dir
        return json.dumps({
            "status": "ok",
            "path":   str(sm_dir),
            "files":  list(inputs["files"].keys()),
        })

    # ── deploy_semantic_model ─────────────────────────────────────────────────
    if name == "deploy_semantic_model":
        import time as _time
        if not state.sm_dir:
            return json.dumps({"error": "call write_semantic_model first"})
        for _attempt in range(4):
            try:
                result = json.loads(_m.deploy_semantic_model(
                    workspace_id          = state.workspace_id,
                    display_name          = inputs["display_name"],
                    semantic_model_folder = str(state.sm_dir),
                ))
            except RuntimeError as e:
                if "refreshing" in str(e).lower() and _attempt < 3:
                    print(f"      [deploy_semantic_model] model still refreshing, waiting 15s…")
                    _time.sleep(15)
                    continue
                return json.dumps({"error": str(e)})
            if "id" in result:
                state.sm_id = result["id"]
            return json.dumps(result)
        return json.dumps({"error": "deploy failed after retries — model still refreshing"})

    # ── refresh_semantic_model ────────────────────────────────────────────────
    if name == "refresh_semantic_model":
        return _m.refresh_semantic_model(
            workspace_id      = state.workspace_id,
            semantic_model_id = inputs["semantic_model_id"],
        )

    # ── execute_dax_query ─────────────────────────────────────────────────────
    if name == "execute_dax_query":
        return _m.execute_dax_query(
            workspace_id      = state.workspace_id,
            semantic_model_id = inputs["semantic_model_id"],
            dax               = inputs["dax"],
        )

    # ── validate_test_data ────────────────────────────────────────────────────
    if name == "validate_test_data":
        source_name = inputs["source_name"]
        ground_truth = _sas_columns(state.data_sources, source_name)
        if not ground_truth:
            return json.dumps({"error": f"source '{source_name}' not found in data_sources.json"})
        loaded = state.loaded_columns.get(source_name, set())
        missing_from_delta = sorted(ground_truth - loaded)
        extra_in_delta     = sorted(loaded - ground_truth)
        ok = not missing_from_delta and not extra_in_delta
        return json.dumps({
            "ok":                ok,
            "source":            source_name,
            "sas_column_count":  len(ground_truth),
            "delta_column_count": len(loaded),
            "missing_from_delta": missing_from_delta,
            "extra_in_delta":    extra_in_delta,
            "message": "All SAS columns present in Delta table." if ok
                       else f"{len(missing_from_delta)} SAS columns missing from Delta; "
                            f"{len(extra_in_delta)} extra columns in Delta — fix generate_test_data.",
        })

    # ── validate_semantic_model ───────────────────────────────────────────────
    if name == "validate_semantic_model":
        import re as _re
        if not state.sm_dir:
            return json.dumps({"error": "call write_semantic_model first"})

        # Collect sourceColumn values per table file
        tables_dir = state.sm_dir / "definition" / "tables"
        tmdl_source_cols: dict[str, set] = {}  # label → set of sourceColumn values
        if tables_dir.exists():
            for tmdl_file in tables_dir.glob("*.tmdl"):
                content = tmdl_file.read_text(encoding="utf-8")
                # Extract first "table <Label>" line
                table_match = _re.search(r"^table\s+'?([^'\n]+)'?", content, _re.MULTILINE)
                if not table_match:
                    continue
                table_label = table_match.group(1).strip()
                cols = set(_re.findall(r"sourceColumn:\s*(.+)", content))
                cols = {c.strip().strip("'\"") for c in cols}
                tmdl_source_cols[table_label] = cols

        # Match canonical sources to TMDL tables and diff against data_sources.json
        results = []
        for src in state.canonical.get("semantic_model", {}).get("sources", []):
            sas_name  = src["name"]
            sas_label = src["label"]
            ground_truth = _sas_columns(state.data_sources, sas_name)
            if not ground_truth:
                continue  # skip non-relational sources (e.g. EsriMapProvider)

            # Find matching TMDL table (by label)
            tmdl_cols = tmdl_source_cols.get(sas_label, set())
            if not tmdl_cols:
                # Try all table files for a rough match
                for tbl, cols in tmdl_source_cols.items():
                    if sas_label.lower() in tbl.lower() or tbl.lower() in sas_label.lower():
                        tmdl_cols = cols
                        break

            missing_from_tmdl = sorted(ground_truth - tmdl_cols)
            extra_in_tmdl     = sorted(tmdl_cols - ground_truth)
            ok = not missing_from_tmdl and not extra_in_tmdl
            results.append({
                "source":           sas_name,
                "label":            sas_label,
                "ok":               ok,
                "sas_columns":      len(ground_truth),
                "tmdl_columns":     len(tmdl_cols),
                "missing_from_tmdl": missing_from_tmdl,
                "extra_in_tmdl":    extra_in_tmdl,
            })

        all_ok = all(r["ok"] for r in results)
        return json.dumps({
            "ok":      all_ok,
            "sources": results,
            "message": "All TMDL sourceColumns match SAS ground truth." if all_ok
                       else "TMDL has column mismatches vs SAS ground truth — fix write_semantic_model.",
        })

    # ── finish ────────────────────────────────────────────────────────────────
    if name == "finish":
        sm_id = inputs.get("semantic_model_id") or state.sm_id
        manifest = {
            "source_report_id":    state.source_report_id,
            "report_name":         state.report_name,
            "workspace_id":        state.workspace_id,
            "lakehouse_id":        state.lakehouse_id,
            "lakehouse_name":      state.lakehouse_name,
            "sql_endpoint":        state.sql_endpoint,
            "sql_database":        state.sql_database,
            "semantic_model_id":   sm_id,
            "semantic_model_name": inputs.get("semantic_model_name", state.safe_name),
            "source_tables":       state.loaded_tables,
            "deployed_measures":   inputs.get("deployed_measures", []),
        }
        manifest_path = state.output_dir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

        print(f"\n{'='*60}")
        print(f"  ✓  {inputs['summary']}")
        print(f"  Semantic Model : {sm_id}")
        print(f"  Manifest       : {manifest_path}")
        print(f"{'='*60}")
        return json.dumps({"status": "done", "manifest_path": str(manifest_path)})

    return json.dumps({"error": f"unknown tool: {name}"})


# ── System prompt ─────────────────────────────────────────────────────────────

SYSTEM = """\
You are a Power BI Semantic Model agent (dev mode). Your job is to:
  1. Understand the canonical report model (SAS VA migration)
  2. Create a Fabric Lakehouse and load realistic synthetic test data for each source table
  3. Generate a TMDL Semantic Model (Import mode via SQL Analytics Endpoint)
  4. Deploy, refresh, and verify with DAX queries
  5. Write a manifest for the Report Agent (Agent 3b)

WORKFLOW — call tools in this exact order:
  1. read_canonical_model(section="overview")
  2. setup_lakehouse(display_name="lh_<safe_report_name>")
  3. read_canonical_model(section="filters")
       — get all static filter definitions with required column values
       — CRITICAL: these values MUST appear in the test data you generate
  4. For each RELATIONAL source (skip map providers):
       a. read_canonical_model(section="source", name=<source_name>)
       b. {DATA_STEP}  ← use filter values from step 3 for the correct columns
       c. validate_test_data(source_name)
            — if ok=false: fix and retry the data step, then validate again
            — DO NOT proceed until ok=true
  5. read_canonical_model(section="metrics")
  6. read_canonical_model(section="overview") — re-read to get parameters[] with range/default
     and hierarchies[] with levels. Use exact range.min/max/default for parameter tables.
     Use hierarchy level names → resolve to dimension.expr for hierarchy blocks in TMDL.
  7. write_semantic_model(files)
       — include hierarchy blocks in each source table .tmdl (see HIERARCHY TMDL RULES)
       — include parameter tables with correct min/max/default from canonical model
  8. validate_semantic_model()
       — if ok=false: fix TMDL (missing sourceColumns) and retry write_semantic_model, then validate again
       — DO NOT proceed until ok=true
  8. deploy_semantic_model(display_name)
  9. refresh_semantic_model(semantic_model_id)
  10. execute_dax_query(semantic_model_id, dax) — verify COUNTROWS and a key measure
  11. finish(summary, semantic_model_id, semantic_model_name, deployed_measures)

If a tool returns an error: analyse it, fix, and retry that step.

════════════════════════════════════════════════════════════
TEST DATA GENERATION RULES
════════════════════════════════════════════════════════════
generate_test_data columns: use dimension.expr names + measure.expr names from the source.
generate_test_data rows: generate exactly 60 rows as a list of dicts.
  - 60 rows is required — do not use fewer, do not exceed this (output token limit).
  - CRITICAL: Before generating, call read_canonical_model("filters") to get the
    filter definitions. For every string dimension that appears in a canonical filter,
    the test data MUST include exactly those filter values in that column.
    Example: if filter_storechain_grand has values=["GRAND"], the Storechain column
    must contain "GRAND" rows (not invented values like "FreshMart").
  - Distribute filter values roughly evenly (e.g. 3 banners → ~67 rows each)
  - Numeric measures: realistic positive floats/ints consistent with the measure description
    Keep magnitudes reasonable: Sales per transaction ~100-500, mkt_bdgt per transaction
    should be much smaller than Sales (e.g. 1-15% of Sales), Cost < Sales, Margin = Sales-Cost
  - Geo columns (_Lat/_Long): realistic lat/lon floats for the named geography
    (e.g. City_Lat/City_Long match the City value, State_Lat/Long match State, etc.)
  - Date dimensions: use ISO strings like "2024-01-15"
  - Call generate_test_data once per source table — do not split into multiple calls.

════════════════════════════════════════════════════════════
TMDL GENERATION RULES  (Import mode, SQL Analytics Endpoint)
════════════════════════════════════════════════════════════
After setup_lakehouse you have: sql_endpoint, sql_database.
After generate_test_data you have: table_name per source.
Use these exact values in the TMDL partition queries.

File layout (write_semantic_model files dict):
  "definition.pbism"              → {"version": "4.0", "settings": {}}
  "definition/database.tmdl"      → database block
  "definition/model.tmdl"         → model block + ref table lines
  "definition/tables/<Label>.tmdl" → one per source table + one per parameter

Indentation: 4 spaces (no tabs). Strings with spaces: single quotes.

definition/database.tmdl:
    database <SafeModelName>
        compatibilityLevel: 1567

definition/model.tmdl:
    model Model
        defaultPowerBIDataSourceVersion: powerBI_V3
        culture: en-US
        discourageImplicitMeasures

    ref table <TableLabel>     ← one per source table
    ref table '<ParamLabel>'   ← one per parameter (single-quoted if spaces)

Source table file (definition/tables/<SourceLabel>.tmdl):
    table <SourceLabel>
        lineageTag: <uuid>

        column <DimCol>                    ← string/date/geo dimension
            dataType: string | dateTime
            lineageTag: <uuid>
            sourceColumn: <DimCol>
            summarizeBy: none

        column <NumericCol>               ← numeric column: visible, aggregatable
            dataType: double | int64
            lineageTag: <uuid>
            sourceColumn: <NumericCol>
            summarizeBy: sum              ← use the same aggregation as measures[].agg
                                            (sum | average | count | min | max)

        measure '<MetricLabel>' = <DAX>
            lineageTag: <uuid>
            formatString: $ #,##0 | 0.00% | #,##0

        partition '<SourceLabel>-Partition' = m
            mode: import
            source =
                let
                    Source = Sql.Database("<sql_endpoint>", "<sql_database>"),
                    Table  = Source{[Schema="dbo", Item="<table_name>"]}[Data]
                in
                    Table

        annotation PBI_ResultType = Table

Parameter table file (definition/tables/'<ParamLabel>'.tmdl):
    IMPORTANT: read parameters[].range.min, parameters[].range.max, parameters[].default
    from the canonical model — use the EXACT values, do not invent them.

    table '<ParamLabel>'
        lineageTag: <uuid>

        column '<ParamLabel>'
            dataType: double
            lineageTag: <uuid>
            sourceColumn: '<ParamLabel>'
            summarizeBy: none

        measure '<ParamLabel> Value' = SELECTEDVALUE('<ParamLabel>'['<ParamLabel>'], <parameters[].default>)
            lineageTag: <uuid>
            formatString: 0.00

        partition '<ParamLabel>-Partition' = m
            mode: import
            source =
                let
                    Source = Table.FromRows(
                        List.Transform(
                            List.Numbers(0, Number.RoundUp((<parameters[].range.max> - (<parameters[].range.min>)) / 0.01) + 1),
                            each {Number.Round(<parameters[].range.min> + _ * 0.01, 2)}
                        ),
                        type table [#"<ParamLabel>" = number]
                    )
                in
                    Source

        CRITICAL: NEVER use GENERATESERIES() — it does not exist in M/Power Query.
        ALWAYS use the Table.FromRows + List.Numbers pattern shown above.

════════════════════════════════════════════════════════════
HIERARCHY TMDL RULES
════════════════════════════════════════════════════════════
The canonical model has semantic_model.hierarchies[]. Each hierarchy maps to ONE source table.
Add a hierarchy block inside the source table's .tmdl file, AFTER all column/measure blocks
and BEFORE the partition block.

hierarchy '<HierarchyLabel>'
    lineageTag: <uuid>

    level <Level0ColumnName>
        lineageTag: <uuid>
        column: <Level0ColumnName>

    level <Level1ColumnName>
        lineageTag: <uuid>
        column: <Level1ColumnName>

Rules:
  - hierarchy.name/label comes from canonical_model.semantic_model.hierarchies[].label
  - hierarchy.levels[] lists dimension names in coarse→fine order
  - Each level's column: value = the dimension.expr for that dimension name
    (look up semantic_model.dimensions where name == level_name, use dimension.expr)
  - The hierarchy goes in the table file for hierarchy.source
    (e.g. if hierarchy.source == "rand_retaildemo" → put in RAND_RETAILDEMO.tmdl)
  - Generate a fresh UUID (lineageTag) for each hierarchy and each level
  - Multiple hierarchies in the same table are fine — add them all


DAX translation per metric type:
  simple   → SUM/AVERAGE/COUNT/MIN/MAX('Table'[measure.expr])
             per measures[type_params.measure].agg
  ratio    → DIVIDE([Numerator Label], [Denominator Label])
  derived  → translate pseudo-math: metric_name → [Metric Label],
             param_name → [Param Label Value], x/y → DIVIDE(x,y)

Format strings: currency → $ #,##0  |  percentage → 0.00%  |  number → #,##0.##

════════════════════════════════════════════════════════════
VALIDATION RULES  (ground truth = SAS data_sources.json)
════════════════════════════════════════════════════════════
validate_test_data checks that EVERY SAS column (xref) appears in the Delta table.
  - missing_from_delta: add these columns to generate_test_data columns[] and rows{}
  - extra_in_delta: remove these columns (likely typos or invented names)
  - Do not proceed past validate_test_data until ok=true for every source.

validate_semantic_model checks that EVERY SAS column (xref) appears as a sourceColumn in TMDL.
  - missing_from_tmdl: add column blocks with correct dataType and summarizeBy
  - extra_in_tmdl: remove column blocks that reference non-existent SAS columns
  - Do not proceed to deploy until ok=true for all sources.

════════════════════════════════════════════════════════════
FINISH
════════════════════════════════════════════════════════════
deployed_measures: list of ALL DAX measure display labels you created in TMDL.
These are passed to Agent 3b so it can reference them by name in the report.
"""

# ── Tool definitions ──────────────────────────────────────────────────────────

TOOLS = [
    {
        "name": "read_canonical_model",
        "description": (
            "Read a section of the canonical model. "
            "Sections: 'overview' | 'source' (name=source_name) | "
            "'metrics' | 'page' (page_id=page_id) | 'filters' (returns all static "
            "filter definitions with resolved column names and required values — "
            "call before generate_test_data to know which column values MUST appear in the data)"
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "section": {"type": "string"},
                "name":    {"type": "string", "description": "source name (for section=source)"},
                "page_id": {"type": "string", "description": "page id (for section=page)"},
            },
            "required": ["section"],
        },
    },
    {
        "name": "setup_lakehouse",
        "description": (
            "Get or create a Fabric Lakehouse for test data. "
            "Returns {lakehouse_id, sql_endpoint, sql_database}."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "display_name": {
                    "type": "string",
                    "description": "Lakehouse name, e.g. lh_retail_insights",
                },
            },
            "required": ["display_name"],
        },
    },
    {
        "name": "generate_test_data",
        "description": (
            "Generate synthetic test data and load it as a Delta table in the Lakehouse. "
            "Pass columns (schema) and rows (the actual data to load)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "source_name": {
                    "type": "string",
                    "description": "Canonical source name (e.g. rand_retaildemo)",
                },
                "table_name": {
                    "type": "string",
                    "description": "Delta table name to create in Fabric (lowercase, underscores)",
                },
                "columns": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": {"type": "string"},
                            "type": {"type": "string",
                                     "description": "string|int64|double|boolean|dateTime"},
                        },
                        "required": ["name", "type"],
                    },
                    "description": "Column schema for the table",
                },
                "rows": {
                    "type": "array",
                    "items": {"type": "object"},
                    "description": "30–50 realistic rows as a list of dicts",
                },
            },
            "required": ["source_name", "table_name", "columns", "rows"],
        },
    },
    {
        "name": "write_semantic_model",
        "description": (
            "Write TMDL files to disk. Include definition.pbism, "
            "definition/database.tmdl, definition/model.tmdl, "
            "and definition/tables/<Name>.tmdl for each table."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "files": {
                    "type": "object",
                    "description": "{relative_path: file_content_string}",
                    "additionalProperties": {"type": "string"},
                },
            },
            "required": ["files"],
        },
    },
    {
        "name": "deploy_semantic_model",
        "description": "Deploy the written semantic model to Fabric. Returns {id, displayName, created}.",
        "input_schema": {
            "type": "object",
            "properties": {
                "display_name": {"type": "string"},
            },
            "required": ["display_name"],
        },
    },
    {
        "name": "refresh_semantic_model",
        "description": "Trigger a refresh. Returns {status: ok} or {status: error}.",
        "input_schema": {
            "type": "object",
            "properties": {
                "semantic_model_id": {"type": "string"},
            },
            "required": ["semantic_model_id"],
        },
    },
    {
        "name": "execute_dax_query",
        "description": "Run a DAX query to verify data. E.g. EVALUATE ROW(\"n\", COUNTROWS(TableName))",
        "input_schema": {
            "type": "object",
            "properties": {
                "semantic_model_id": {"type": "string"},
                "dax":               {"type": "string"},
            },
            "required": ["semantic_model_id", "dax"],
        },
    },
    {
        "name": "download_cas_table",
        "description": (
            "Download a real CAS table from SAS Viya and load it as a Delta table "
            "in the Lakehouse. Use instead of generate_test_data when --use-real-data is set. "
            "Returns {status, table_name, rows_loaded, columns}."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "source_name": {
                    "type": "string",
                    "description": "Canonical source name (e.g. ds10 or RAND_RETAILDEMO)",
                },
                "table_name": {
                    "type": "string",
                    "description": "Delta table name to create in Fabric (lowercase, underscores)",
                },
                "max_rows": {
                    "type": "integer",
                    "description": "Max rows to download (default 10000). CAS returns 1000 per call.",
                },
            },
            "required": ["source_name", "table_name"],
        },
    },
    {
        "name": "validate_test_data",
        "description": (
            "Validate a loaded Delta table against SAS data_sources.json (ground truth). "
            "Returns {ok, missing_from_delta, extra_in_delta}. "
            "Call after generate_test_data for each source. Fix and re-call generate_test_data if not ok."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "source_name": {
                    "type": "string",
                    "description": "Canonical source name (e.g. ds10 or rand_retaildemo)",
                },
            },
            "required": ["source_name"],
        },
    },
    {
        "name": "validate_semantic_model",
        "description": (
            "Validate TMDL sourceColumn: values against SAS data_sources.json (ground truth). "
            "Returns {ok, sources: [{missing_from_tmdl, extra_in_tmdl}]}. "
            "Call after write_semantic_model. Fix and re-call write_semantic_model if not ok."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    {
        "name": "finish",
        "description": (
            "Write manifest.json and signal completion. "
            "deployed_measures must list ALL DAX measure labels created in TMDL."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "summary":             {"type": "string"},
                "semantic_model_id":   {"type": "string"},
                "semantic_model_name": {"type": "string"},
                "deployed_measures":   {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Display labels of all DAX measures in the TMDL",
                },
            },
            "required": ["summary", "semantic_model_id", "deployed_measures"],
        },
    },
]


# ── Agentic loop ──────────────────────────────────────────────────────────────

def _build_system(use_real_data: bool) -> str:
    if use_real_data:
        data_step = (
            "download_cas_table(source_name, table_name)\n"
            "            — downloads the real CAS table from SAS Viya → uploads to Delta\n"
            "            — no columns/rows needed: the real schema is discovered automatically"
        )
    else:
        data_step = (
            "generate_test_data(source_name, table_name, columns, rows)\n"
            "            — columns MUST include EVERY column from the SAS source (all xref values)\n"
            "            — generate exactly 20 realistic rows as a list of dicts"
        )
    return SYSTEM.replace("{DATA_STEP}", data_step)


def run_agent(state: _State) -> None:
    client    = anthropic.Anthropic()
    MAX_TURNS = 60

    user_msg = f"""
Canonical report model to migrate:

```json
{json.dumps(state.canonical.get("report", {}), indent=2)}
```

Source report id : {state.source_report_id}
Fabric workspace : {state.workspace_id}

Use the tools to: read the canonical model, create test data in Fabric,
build the semantic model, deploy and verify it, then call finish.
"""
    messages = [{"role": "user", "content": user_msg}]

    for turn in range(MAX_TURNS):
        print(f"\n[turn {turn + 1}]")
        for _api_attempt in range(5):
            try:
                response = client.messages.create(
                    model=MODEL,
                    max_tokens=MAX_TOKENS,
                    system=_build_system(state.use_real_data),
                    tools=TOOLS,
                    messages=messages,
                )
                break
            except anthropic.APIStatusError as e:
                if e.status_code in (529, 503, 500) and _api_attempt < 4:
                    wait = 20 * (_api_attempt + 1)
                    print(f"  [API {e.status_code}] retrying in {wait}s…")
                    import time as _t; _t.sleep(wait)
                else:
                    raise
        print(f"  stop={response.stop_reason}  "
              f"in={response.usage.input_tokens}  out={response.usage.output_tokens}")

        for block in response.content:
            if hasattr(block, "text") and block.text.strip():
                print(f"  [Claude] {block.text.strip()[:300]}")

        messages.append({"role": "assistant", "content": response.content})

        if response.stop_reason == "end_turn":
            print("  Agent finished (end_turn).")
            break

        if response.stop_reason == "max_tokens":
            # If there are pending tool_use blocks, return error results for them
            # (can't send a user message without corresponding tool_results)
            pending_tools = [b for b in response.content if b.type == "tool_use"]
            if pending_tools:
                print(f"  [max_tokens] {len(pending_tools)} pending tool_use — returning truncation errors")
                results = []
                for block in pending_tools:
                    results.append({
                        "type":        "tool_result",
                        "tool_use_id": block.id,
                        "content":     (
                            "ERROR: Your response was truncated (max_tokens). "
                            "Generate data for ONE source table at a time with max 60 rows. "
                            "Do NOT batch multiple source tables in a single call."
                        ),
                        "is_error":    True,
                    })
                messages.append({"role": "user", "content": results})
            else:
                messages.append({"role": "user", "content": "Continue exactly where you left off."})
            continue

        if response.stop_reason == "tool_use":
            results = []
            done    = False
            for block in response.content:
                if block.type != "tool_use":
                    continue
                result = _execute(block.name, block.input, state)
                print(f"      ← {result[:200]}")
                results.append({
                    "type":        "tool_result",
                    "tool_use_id": block.id,
                    "content":     result,
                })
                if block.name == "finish":
                    done = True
            messages.append({"role": "user", "content": results})
            if done:
                break
        else:
            print(f"  Unexpected stop_reason: {response.stop_reason}")
            break
    else:
        print(f"  WARNING: reached MAX_TURNS ({MAX_TURNS}).")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Agent 3a — Semantic Model Agent")
    parser.add_argument("--report-id",    default="cbf97b0a-457d-4b4f-8913-547e0cdf390c")
    parser.add_argument("--input-dir",    default="")
    parser.add_argument("--workspace-id", default=os.environ.get("FABRIC_WORKSPACE_ID", ""))
    parser.add_argument("--use-real-data", action="store_true", default=True,
                        help="Download real parquet from SAS Viya CAS (default)")
    parser.add_argument("--use-synthetic-data", action="store_true",
                        help="Generate synthetic test data instead of downloading from CAS")
    args = parser.parse_args()

    if not args.workspace_id:
        sys.exit("ERROR: set FABRIC_WORKSPACE_ID or pass --workspace-id")

    use_real_data = not args.use_synthetic_data

    if use_real_data:
        if not os.environ.get("VIYA_USERNAME") or not os.environ.get("VIYA_PASSWORD"):
            sys.exit("ERROR: real data mode requires VIYA_USERNAME and VIYA_PASSWORD (use --use-synthetic-data to override)")

    input_dir      = Path(args.input_dir or f"docs/{args.report_id}")
    canonical_path = input_dir / "canonical_model.json"
    if not canonical_path.exists():
        sys.exit(f"ERROR: {canonical_path} not found. Run agent2_canonical.py first.")

    canonical   = json.loads(canonical_path.read_text(encoding="utf-8"))
    report_name = canonical.get("report", {}).get("name", "Report")
    safe_name   = report_name.replace(" ", "_")
    output_dir  = Path(OUTPUT_BASE) / safe_name
    output_dir.mkdir(parents=True, exist_ok=True)

    # Ground truth: SAS data_sources.json
    ds_path = input_dir / "data_sources.json"
    if not ds_path.exists():
        sys.exit(f"ERROR: {ds_path} not found — needed for column validation.")
    data_sources = json.loads(ds_path.read_text(encoding="utf-8")).get("data_sources", [])

    print(f"\n{'='*60}")
    print(f"  Agent 3a — Semantic Model Agent")
    print(f"  Report    : {report_name}")
    print(f"  Workspace : {args.workspace_id}")
    print(f"  Input     : {canonical_path}")
    print(f"  Output    : {output_dir}/")
    print(f"  Data mode : {'REAL (CAS download)' if use_real_data else 'SYNTHETIC (generated)'}")
    print(f"{'='*60}")

    auth = json.loads(_m.authenticate())
    if auth.get("fabric") != "ok":
        sys.exit(f"ERROR: Fabric auth failed: {auth}")
    print(f"  Auth: fabric={auth['fabric']}  storage={auth['storage']}\n")

    state = _State(canonical, args.workspace_id, output_dir, args.report_id,
                   data_sources, use_real_data=use_real_data)
    run_agent(state)


if __name__ == "__main__":
    main()
