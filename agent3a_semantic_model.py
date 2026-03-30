#!/usr/bin/env python3
"""
Agent 3a — Semantic Model Agent
=================================
Part of the SAS Visual Analytics → Power BI migration pipeline (dev mode).

Reads canonical_model.json (Agent 2 output) and:
  1. Creates a Fabric Lakehouse for test data
  2. Generates realistic synthetic test data per source table → loads to Lakehouse
  3. Generates TMDL (Import mode via SQL Analytics Endpoint)
  4. Deploys Semantic Model to Fabric
  5. Refreshes + verifies with DAX queries
  6. Writes manifest.json → picked up by Agent 3b

Usage:
    python agent3a_semantic_model.py [--report-id UUID] [--input-dir PATH]

Environment:
    ANTHROPIC_API_KEY
    FABRIC_WORKSPACE_ID
    FABRIC_TENANT_ID, FABRIC_CLIENT_ID, FABRIC_CLIENT_SECRET
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

# ── Helpers ───────────────────────────────────────────────────────────────────

def make_id() -> str:
    return uuid.uuid4().hex[:20]


# ── Canonical model reader ────────────────────────────────────────────────────

def _read_canonical(canonical: dict, section: str,
                    name: str = None, page_id: str = None) -> str:
    sm = canonical.get("semantic_model", {})

    if section == "overview":
        return json.dumps({
            "report":     canonical.get("report", {}),
            "sources":    [{"name": s["name"], "label": s["label"]}
                           for s in sm.get("sources", [])],
            "dimensions": [d["name"] for d in sm.get("dimensions", [])],
            "measures":   [m["name"] for m in sm.get("measures", [])],
            "metrics":    [m["name"] for m in sm.get("metrics", [])],
            "parameters": [p["name"] for p in sm.get("parameters", [])],
            "pages":      [{"id": p["id"],
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

    return json.dumps({"error": f"unknown section '{section}'"})


# ── Runtime state ─────────────────────────────────────────────────────────────

class _State:
    def __init__(self, canonical: dict, workspace_id: str,
                 output_dir: Path, source_report_id: str):
        self.canonical        = canonical
        self.workspace_id     = workspace_id
        self.output_dir       = output_dir
        self.source_report_id = source_report_id
        self.report_name      = canonical.get("report", {}).get("name", "Report")
        self.safe_name        = self.report_name.replace(" ", "_")

        # set by setup_lakehouse
        self.lakehouse_id:   str = ""
        self.lakehouse_name: str = ""
        self.sql_endpoint:   str = ""
        self.sql_database:   str = ""

        # accumulated by generate_test_data: source_name → table_name in Fabric
        self.loaded_tables: dict = {}

        # set by write_semantic_model
        self.sm_dir: Path | None = None

        # set by deploy_semantic_model
        self.sm_id: str = ""


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
        lh = json.loads(_m.get_or_create_lakehouse(
            workspace_id=state.workspace_id,
            display_name=inputs["display_name"],
        ))
        lh_id = lh["id"]
        details = json.loads(_m.get_lakehouse(state.workspace_id, lh_id))
        props   = details.get("properties", {})
        sql_ep  = props.get("sqlEndpointProperties", {})
        conn    = sql_ep.get("connectionString", "")

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
            state.loaded_tables[source_name] = table_name
            return json.dumps({
                "status":        "ok",
                "table_name":    table_name,
                "rows_loaded":   result["rows"],
                "delta_version": result["delta_version"],
            })
        return json.dumps(result)

    # ── write_semantic_model ──────────────────────────────────────────────────
    if name == "write_semantic_model":
        sm_dir = state.output_dir / f"{state.safe_name}.SemanticModel"
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
        if not state.sm_dir:
            return json.dumps({"error": "call write_semantic_model first"})
        result = json.loads(_m.deploy_semantic_model(
            workspace_id             = state.workspace_id,
            display_name             = inputs["display_name"],
            semantic_model_folder    = str(state.sm_dir),
        ))
        if "id" in result:
            state.sm_id = result["id"]
        return json.dumps(result)

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
  3. For each source:
       a. read_canonical_model(section="source", name=<source_name>)
       b. generate_test_data(source_name, table_name, columns, rows)
  4. read_canonical_model(section="metrics")
  5. write_semantic_model(files)
  6. deploy_semantic_model(display_name)
  7. refresh_semantic_model(semantic_model_id)
  8. execute_dax_query(semantic_model_id, dax) — verify COUNTROWS and a key measure
  9. finish(summary, semantic_model_id, semantic_model_name, deployed_measures)

If a tool returns an error: analyse it, fix, and retry that step.

════════════════════════════════════════════════════════════
TEST DATA GENERATION RULES
════════════════════════════════════════════════════════════
generate_test_data columns: use dimension.expr names + measure.expr names from the source.
generate_test_data rows: generate 30–50 realistic rows as a list of dicts.
  - Numeric measures: realistic positive floats/ints consistent with the measure description
  - String dimensions: realistic categorical values (3–6 distinct values, repeated across rows)
  - Date dimensions: use ISO strings like "2024-01-15"
  - Keep numeric relationships sensible (e.g. Cost < Sales, Margin = Sales - Cost)

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

        column <DimExpr>
            dataType: string | int64 | double | dateTime | boolean
            lineageTag: <uuid>
            sourceColumn: <DimExpr>
            summarizeBy: none

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
    table '<ParamLabel>'
        lineageTag: <uuid>

        column '<ParamLabel>'
            dataType: double
            lineageTag: <uuid>
            sourceColumn: '<ParamLabel>'
            summarizeBy: none

        measure '<ParamLabel> Value' = SELECTEDVALUE('<ParamLabel>'['<ParamLabel>'], <default>)
            lineageTag: <uuid>
            formatString: 0.00

        partition '<ParamLabel>-Partition' = m
            mode: import
            source =
                GENERATESERIES(<min>, <max>, 0.01)

DAX translation per metric type:
  simple   → SUM/AVERAGE/COUNT/MIN/MAX('Table'[measure.expr])
             per measures[type_params.measure].agg
  ratio    → DIVIDE([Numerator Label], [Denominator Label])
  derived  → translate pseudo-math: metric_name → [Metric Label],
             param_name → [Param Label Value], x/y → DIVIDE(x,y)

Format strings: currency → $ #,##0  |  percentage → 0.00%  |  number → #,##0.##

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
            "'metrics' | 'page' (page_id=page_id)"
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
        response = client.messages.create(
            model=MODEL,
            max_tokens=MAX_TOKENS,
            system=SYSTEM,
            tools=TOOLS,
            messages=messages,
        )
        print(f"  stop={response.stop_reason}  "
              f"in={response.usage.input_tokens}  out={response.usage.output_tokens}")

        for block in response.content:
            if hasattr(block, "text") and block.text.strip():
                print(f"  [Claude] {block.text.strip()[:300]}")

        messages.append({"role": "assistant", "content": response.content})

        if response.stop_reason == "end_turn":
            print("  Agent finished (end_turn).")
            break

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
    args = parser.parse_args()

    if not args.workspace_id:
        sys.exit("ERROR: set FABRIC_WORKSPACE_ID or pass --workspace-id")

    input_dir      = Path(args.input_dir or f"docs/{args.report_id}")
    canonical_path = input_dir / "canonical_model.json"
    if not canonical_path.exists():
        sys.exit(f"ERROR: {canonical_path} not found. Run agent2_canonical.py first.")

    canonical   = json.loads(canonical_path.read_text(encoding="utf-8"))
    report_name = canonical.get("report", {}).get("name", "Report")
    safe_name   = report_name.replace(" ", "_")
    output_dir  = Path(OUTPUT_BASE) / safe_name
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n{'='*60}")
    print(f"  Agent 3a — Semantic Model Agent")
    print(f"  Report    : {report_name}")
    print(f"  Workspace : {args.workspace_id}")
    print(f"  Input     : {canonical_path}")
    print(f"  Output    : {output_dir}/")
    print(f"{'='*60}")

    auth = json.loads(_m.authenticate())
    if auth.get("fabric") != "ok":
        sys.exit(f"ERROR: Fabric auth failed: {auth}")
    print(f"  Auth: fabric={auth['fabric']}  storage={auth['storage']}\n")

    state = _State(canonical, args.workspace_id, output_dir, args.report_id)
    run_agent(state)


if __name__ == "__main__":
    main()
