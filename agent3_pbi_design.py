#!/usr/bin/env python3
"""
Agent 3 — Power BI Design Agent (v2)
======================================
Part of the SAS Visual Analytics → Power BI migration pipeline.

Reads canonical_model.json (Agent 2 output) and:
  1. Generates TMDL files for the Semantic Model
  2. Generates a structured report visual spec (pages + visuals)
  3. Deploys and tests everything on Fabric using an agentic tool-use loop:
       write_semantic_model  → deploy_semantic_model → refresh → execute_dax_query
       → write_report_spec  → deploy_report → export_report_page → finish

Claude self-corrects: if deployment fails it reads the error, fixes the files,
and retries — no manual intervention needed.

Usage:
    python agent3_pbi_design.py [--report-id UUID] [--input-dir PATH]

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
from pathlib import Path

import anthropic
import importlib.util

# ── Load Fabric MCP server ────────────────────────────────────────────────────

_spec = importlib.util.spec_from_file_location(
    "fabric_server",
    Path(__file__).parent / "fabric_mcp" / "server.py",
)
_m = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_m)

# ── Constants ─────────────────────────────────────────────────────────────────

MODEL      = "claude-opus-4-6"
MAX_TOKENS = 16000
OUTPUT_BASE = "pbi"

# ── Helpers ───────────────────────────────────────────────────────────────────

def make_id() -> str:
    return uuid.uuid4().hex[:20]


# ── PBIR-Legacy builder ───────────────────────────────────────────────────────
# Claude outputs a clean structured spec; Python does the JSON stringification.

# Maps visual type → (category_role, value_role)
_ROLE_MAP = {
    "columnChart":                    ("Category", "Y"),
    "clusteredColumnChart":           ("Category", "Y"),
    "clusteredBarChart":              ("Category", "Y"),
    "barChart":                       ("Category", "Y"),
    "lineChart":                      ("Axis",     "Y"),
    "pieChart":                       ("Category", "Y"),
    "donutChart":                     ("Category", "Y"),
    "card":                           ("Values",   None),
    "treemap":                        ("Group",    "Values"),
    "tableEx":                        ("Values",   None),
    "pivotTable":                     ("Rows",     "Values"),
    "scatterChart":                   ("X",        "Y"),
    "lineClusteredColumnComboChart":  ("Category", "Y"),
}

# Aggregation function codes
_AGG = {"sum": 0, "avg": 1, "count": 2, "min": 3, "max": 4}


def _build_visual_config(vis: dict, source_table: str) -> dict:
    """
    Convert one visual spec entry into a PBIR-Legacy singleVisual config dict.

    vis fields:
        type        visualType string (e.g. "columnChart")
        dim_col     dimension/category column name (optional for card)
        measure_col raw column to aggregate (for inline Sum)
        measure_name named DAX measure (alternative to measure_col)
        agg         aggregation name: "sum"|"avg"|"count"|"min"|"max" (default "sum")
        position    {x, y, width, height, z, tabOrder}
    """
    vtype    = vis["type"]
    dim_col  = vis.get("dim_col", "")
    msr_col  = vis.get("measure_col", "")
    msr_name = vis.get("measure_name", "")   # named DAX measure
    agg_fn   = _AGG.get(vis.get("agg", "sum").lower(), 0)

    cat_role, val_role = _ROLE_MAP.get(vtype, ("Category", "Y"))

    projections: dict = {}
    selects: list     = []
    order_by: list    = []
    frm = [{"Name": "c", "Entity": source_table, "Type": 0}]

    # ── dimension / category ──────────────────────────────────────────────────
    if dim_col:
        q_ref = f"{source_table}.{dim_col}"
        projections[cat_role] = [{"queryRef": q_ref, "active": True}]
        selects.append({
            "Column": {
                "Expression": {"SourceRef": {"Source": "c"}},
                "Property": dim_col,
            },
            "Name": q_ref,
            "NativeReferenceName": dim_col,
        })

    # ── measure / value ───────────────────────────────────────────────────────
    role = val_role or cat_role   # card: val_role is None, use cat_role (Values)

    if msr_name:
        # Named DAX measure (defined in semantic model)
        q_ref = f"{source_table}.{msr_name}"
        projections[role] = [{"queryRef": q_ref}]
        selects.append({
            "Measure": {
                "Expression": {"SourceRef": {"Source": "c"}},
                "Property": msr_name,
            },
            "Name": q_ref,
            "NativeReferenceName": msr_name,
        })
    elif msr_col:
        # Inline aggregation (no explicit DAX measure needed)
        fn_name = vis.get("agg", "Sum").capitalize()
        q_ref   = f"{fn_name}({source_table}.{msr_col})"
        agg_expr = {
            "Aggregation": {
                "Expression": {
                    "Column": {
                        "Expression": {"SourceRef": {"Source": "c"}},
                        "Property": msr_col,
                    }
                },
                "Function": agg_fn,
            }
        }
        projections[role] = [{"queryRef": q_ref}]
        selects.append({
            **agg_expr,
            "Name": q_ref,
            "NativeReferenceName": f"{fn_name} of {msr_col}",
        })
        order_by = [{"Direction": 2, "Expression": agg_expr}]

    proto = {"Version": 2, "From": frm, "Select": selects}
    if order_by:
        proto["OrderBy"] = order_by

    pos = vis.get("position", {})
    return {
        "name": make_id(),
        "layouts": [{
            "id": 0,
            "position": {
                "x":        float(pos.get("x", 10)),
                "y":        float(pos.get("y", 10)),
                "z":        float(pos.get("z", 0)),
                "width":    float(pos.get("width", 600)),
                "height":   float(pos.get("height", 400)),
                "tabOrder": int(pos.get("tabOrder", 0)),
            },
        }],
        "singleVisual": {
            "visualType": vtype,
            "projections": projections,
            "prototypeQuery": proto,
            "drillFilterOtherVisuals": True,
            "hasDefaultSort": bool(order_by),
        },
    }


def build_report_json(spec: dict, source_table: str) -> str:
    """
    Convert a structured report spec into a PBIR-Legacy report.json string.

    spec format:
    {
      "pages": [
        {
          "name": "<hex20>",          ← must be unique
          "displayName": "Overview",
          "visuals": [ { ...visual spec... }, ... ]
        }
      ]
    }
    """
    report_config = {
        "version": "5.70",
        "themeCollection": {
            "baseTheme": {
                "name": "CY26SU02",
                "version": {"visual": "2.6.0", "report": "3.1.0", "page": "2.3.0"},
                "type": 2,
            }
        },
        "activeSectionIndex": 0,
        "defaultDrillFilterOtherVisuals": True,
        "settings": {
            "useNewFilterPaneExperience":     True,
            "allowChangeFilterTypes":         True,
            "useStylableVisualContainerHeader": True,
            "queryLimitOption":               6,
            "useEnhancedTooltips":            True,
            "exportDataMode":                 1,
            "useDefaultAggregateDisplayName": True,
        },
    }

    sections = []
    for i, page in enumerate(spec.get("pages", [])):
        page_name = page.get("name") or make_id()
        containers = []
        for vis in page.get("visuals", []):
            vc = _build_visual_config(vis, source_table)
            pos = vis.get("position", {})
            containers.append({
                "config":  json.dumps(vc),
                "filters": "[]",
                "height":  float(pos.get("height", 400)),
                "width":   float(pos.get("width",  600)),
                "x":       float(pos.get("x", 10)),
                "y":       float(pos.get("y", 10)),
                "z":       float(pos.get("z", 0)),
            })

        section = {
            "config":           "{}",
            "displayName":      page.get("displayName", f"Page {i + 1}"),
            "displayOption":    1,
            "filters":          "[]",
            "height":           720.0,
            "name":             page_name,
            "visualContainers": containers,
            "width":            1280.0,
        }
        if i > 0:
            section["ordinal"] = i
        sections.append(section)

    return json.dumps({
        "config":              json.dumps(report_config),
        "layoutOptimization":  0,
        "resourcePackages":    [],
        "sections":            sections,
    }, indent=2)


# ── System prompt ─────────────────────────────────────────────────────────────

SYSTEM = """\
You are a Power BI migration agent. Your job is to take a canonical report model \
(produced by Agent 2 from a SAS Visual Analytics report) and deliver a working \
Semantic Model + Report deployed on Microsoft Fabric.

WORKFLOW — call tools in this order:
  1. write_semantic_model   — write all TMDL files
  2. deploy_semantic_model  — deploy to Fabric, get the SM id
  3. refresh_semantic_model — verify the model processes without error
  4. execute_dax_query      — confirm data is accessible (e.g. COUNTROWS)
  5. write_report_spec      — describe the pages and visuals
  6. deploy_report          — deploy the report linked to the SM
  7. export_report_page     — verify the first page renders as PNG
  8. finish                 — summarise what was deployed

If a tool returns an error: read it carefully, fix the relevant files/spec, and retry \
that step. Do not skip steps.

════════════════════════════════════════════════════════════
READING THE CANONICAL MODEL
════════════════════════════════════════════════════════════
The canonical model has this structure:

semantic_model:
  sources[]       → data sources (SAS Viya connection: server, library, table)
  dimensions[]    → {name, label, expr, source, type}
                    expr = actual column name in the source table
  measures[]      → {name, label, agg, expr, source, format}
                    base aggregations: expr = column, agg = sum/average/count/min/max
  metrics[]       → {name, label, type, type_params, format}
                    type=simple: type_params.measure → name in measures[]
                    type=ratio:  type_params.numerator + type_params.denominator → metric names
                    type=derived: type_params.expr → pseudo-math expression
  parameters[]    → {name, label, data_type, default, range: {min, max}}
                    what-if parameters → Power BI what-if parameter tables

pages[]:
  display_name    → page tab label
  visuals[]:
    visual_type   → canonical type (see mapping below)
    metrics[]     → list of metric names (references to semantic_model.metrics[])
    dimensions[]  → list of dimension names (references to semantic_model.dimensions[])
    spec          → Vega-Lite spec — FOR REFERENCE ONLY, do not use directly

RESOLVING A VISUAL'S DATA FIELDS:
  For each name in visual.dimensions[]:
    → look up semantic_model.dimensions where name matches
    → dim_col = that dimension's expr  (actual column name)
    → source  = that dimension's source (which source table)

  For each name in visual.metrics[]:
    → look up semantic_model.metrics where name matches
    → if type=simple: look up semantic_model.measures[type_params.measure]
        → measure_col = that measure's expr, agg = that measure's agg
    → if type=ratio/derived: generate a DAX measure (use measure_name in write_report_spec)

  In write_report_spec use:
    measure_name  → when a named DAX measure exists in the TMDL (preferred for complex metrics)
    measure_col + agg → for simple inline aggregations only

════════════════════════════════════════════════════════════
VISUAL TYPE MAPPING  (canonical → Power BI)
════════════════════════════════════════════════════════════
kpi_card        → card
bar_chart       → clusteredBarChart
line_chart      → lineChart
combo_chart     → lineClusteredColumnComboChart
scatter_plot    → scatterChart
pie_chart       → pieChart
treemap         → treemap
table           → tableEx
crosstab        → pivotTable
filter_control  → slicer
map             → tableEx  (no native geo map; fall back to table showing geo dimension + metric)
text            → SKIP     (static text labels — omit from report spec)

════════════════════════════════════════════════════════════
TMDL GENERATION RULES
════════════════════════════════════════════════════════════
File layout inside write_semantic_model:
  definition.pbism           → {"version": "4.0", "settings": {}}
  definition/database.tmdl   → database block
  definition/model.tmdl      → model block + ref table lines
  definition/tables/<T>.tmdl → one file per source table + one per parameter

Indentation: 4 spaces (no tabs). String values with spaces: single quotes.

database.tmdl:
    database <ModelName>
        compatibilityLevel: 1605

model.tmdl:
    model Model
        culture: en-US
        discourageImplicitMeasures

    ref table <TableName>      ← one per source table + one per parameter table

Table file (definition/tables/<SourceLabel>.tmdl):
  - One column per dimension that has this source
  - One DAX measure per metric that has this source:

    measure '<Metric Label>' = <DAX>
        formatString: $ #,##0 | 0.00% | #,##0

  - Partition uses SAS Viya connector:
        partition '<Label>-Partition' = m
            mode: import
            source =
                let
                    Source = SasViya.Dataset("<server>", "<library>", "<table>")
                in
                    Source

DAX translation per metric type:
  simple   (type_params.measure → measures[m])  →  AGG('Table'[measures[m].expr])
           where AGG = SUM/AVERAGE/COUNT/MIN/MAX per measures[m].agg
  ratio    (num/den metric names)               →  DIVIDE([Num Label], [Den Label])
  derived  (pseudo-math expr)                   →  translate to DAX:
             sum(col)      → SUM('Table'[col])
             metric_name   → [Metric Label]
             param_name    → [Param Label Value]
             x / y         → DIVIDE(x, y)
             IF cond THEN a ELSE b → IF(cond, a, b)

Format strings:
  currency   →  $ #,##0
  percentage →  0.00%
  number     →  #,##0.##

What-if parameter table (one per parameters[]):
    table '<Param Label>'
        column '<Param Label>'
            dataType: double
            sourceColumn: '<Param Label>'
            summarizeBy: none

        measure '<Param Label> Value' = SELECTEDVALUE('<Param Label>'['<Param Label>'], <default>)
            formatString: 0.00

        partition '<Param Label>-Partition' = m
            mode: import
            source =
                GENERATESERIES(<min>, <max>, 0.01)

════════════════════════════════════════════════════════════
REPORT SPEC FORMAT  (write_report_spec input)
════════════════════════════════════════════════════════════
{
  "pages": [
    {
      "name": "<hex20>",
      "displayName": "Overview",
      "visuals": [
        {
          "type": "clusteredBarChart",
          "dim_col": "Region_2",
          "measure_name": "Total Sales",
          "position": {"x": 10, "y": 10, "width": 600, "height": 380}
        },
        {
          "type": "card",
          "measure_name": "Total Sales",
          "position": {"x": 620, "y": 10, "width": 280, "height": 120}
        },
        {
          "type": "slicer",
          "dim_col": "ChannelType",
          "position": {"x": 10, "y": 400, "width": 200, "height": 120}
        }
      ]
    }
  ]
}

Rules:
  • Use measure_name (DAX measure label) for all metrics — not measure_col
  • Use dim_col = dimension.expr (the actual source column name)
  • source_table = the source label from semantic_model.sources (e.g. "RAND_RETAILDEMO")
  • Skip text/static visuals
  • Map each page from canonical pages[].display_name
  • Canvas: 1280×720 px, ~10 px gaps
  • Typical sizes: card 280×120, bar/line 580×340, table 780×300, slicer 200×120
"""


# ── Tool definitions ──────────────────────────────────────────────────────────

TOOLS = [
    {
        "name": "write_semantic_model",
        "description": (
            "Write TMDL files for the semantic model to disk. "
            "Include definition.pbism, definition/database.tmdl, definition/model.tmdl, "
            "and one definition/tables/<Name>.tmdl per table."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "files": {
                    "type": "object",
                    "description": "Dict of {relative_path: file_content_string}",
                    "additionalProperties": {"type": "string"},
                }
            },
            "required": ["files"],
        },
    },
    {
        "name": "deploy_semantic_model",
        "description": (
            "Deploy the written semantic model to Fabric. "
            "Returns {id, displayName, created} on success or {error} on failure."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "display_name": {
                    "type": "string",
                    "description": "Name for the semantic model in the Fabric workspace",
                },
            },
            "required": ["display_name"],
        },
    },
    {
        "name": "refresh_semantic_model",
        "description": (
            "Trigger a refresh of the deployed semantic model to verify it processes. "
            "Returns {status: ok} or {status: error, message: ...}."
        ),
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
        "description": (
            "Run a DAX query against the semantic model to verify data is accessible. "
            "Example: EVALUATE ROW(\"n\", COUNTROWS(TableName))"
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "semantic_model_id": {"type": "string"},
                "dax": {"type": "string"},
            },
            "required": ["semantic_model_id", "dax"],
        },
    },
    {
        "name": "write_report_spec",
        "description": (
            "Write a structured report spec. "
            "Python converts this to a PBIR-Legacy report.json automatically — "
            "no manual JSON stringification needed."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "spec": {
                    "type": "object",
                    "description": "Report spec: {pages: [{name, displayName, visuals: [...]}]}",
                },
                "source_table": {
                    "type": "string",
                    "description": "Primary table name in the semantic model (used for prototypeQuery).",
                },
            },
            "required": ["spec", "source_table"],
        },
    },
    {
        "name": "deploy_report",
        "description": (
            "Deploy the written report to Fabric linked to the semantic model. "
            "Returns {id, displayName, created} or {error}."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "display_name":       {"type": "string"},
                "semantic_model_id":  {"type": "string"},
            },
            "required": ["display_name", "semantic_model_id"],
        },
    },
    {
        "name": "export_report_page",
        "description": (
            "Export a report page as PNG to verify it renders. "
            "Returns {status: ok, bytes: N, path: ...} or {status: error, message: ...}."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "report_id":  {"type": "string"},
                "page_name":  {
                    "type": "string",
                    "description": "The page name (hex ID from write_report_spec, not displayName)",
                },
            },
            "required": ["report_id", "page_name"],
        },
    },
    {
        "name": "finish",
        "description": "Signal completion. Provide a human-readable summary of what was deployed.",
        "input_schema": {
            "type": "object",
            "properties": {
                "summary":           {"type": "string"},
                "semantic_model_id": {"type": "string"},
                "report_id":         {"type": "string"},
            },
            "required": ["summary"],
        },
    },
]


# ── Runtime state ─────────────────────────────────────────────────────────────

class _State:
    def __init__(self, report_name: str, workspace_id: str, output_dir: Path):
        self.report_name  = report_name
        self.workspace_id = workspace_id
        self.output_dir   = output_dir
        self.safe_name    = report_name.replace(" ", "_")
        self.sm_dir:  Path | None = None
        self.rpt_dir: Path | None = None
        self.sm_id:    str = ""
        self.report_id: str = ""
        self.first_page_name: str = "ReportSection"


# ── Tool executor ─────────────────────────────────────────────────────────────

def _execute(name: str, inputs: dict, state: _State) -> str:
    """Dispatch a tool call and return the result as a JSON string."""
    short = {k: (str(v)[:60] + "…" if len(str(v)) > 60 else str(v)) for k, v in inputs.items()}
    print(f"    → {name}({json.dumps(short, ensure_ascii=False)})")

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
            "config": {"logicalId": str(uuid.uuid4())},
            "metadata": {"type": "SemanticModel", "displayName": state.safe_name},
        }, indent=2), encoding="utf-8")
        state.sm_dir = sm_dir
        return json.dumps({"status": "ok", "path": str(sm_dir),
                           "files_written": list(inputs["files"].keys())})

    # ── deploy_semantic_model ─────────────────────────────────────────────────
    if name == "deploy_semantic_model":
        if not state.sm_dir:
            return json.dumps({"error": "call write_semantic_model first"})
        result = json.loads(_m.deploy_semantic_model(
            workspace_id=state.workspace_id,
            display_name=inputs["display_name"],
            semantic_model_folder=str(state.sm_dir),
        ))
        if "id" in result:
            state.sm_id = result["id"]
        return json.dumps(result)

    # ── refresh_semantic_model ────────────────────────────────────────────────
    if name == "refresh_semantic_model":
        return _m.refresh_semantic_model(
            workspace_id=state.workspace_id,
            semantic_model_id=inputs["semantic_model_id"],
        )

    # ── execute_dax_query ─────────────────────────────────────────────────────
    if name == "execute_dax_query":
        return _m.execute_dax_query(
            workspace_id=state.workspace_id,
            semantic_model_id=inputs["semantic_model_id"],
            dax=inputs["dax"],
        )

    # ── write_report_spec ─────────────────────────────────────────────────────
    if name == "write_report_spec":
        spec         = inputs["spec"]
        source_table = inputs["source_table"]

        pages = spec.get("pages", [])
        if pages:
            state.first_page_name = pages[0].get("name") or state.first_page_name

        rpt_dir = state.output_dir / f"{state.safe_name}.Report"
        rpt_dir.mkdir(parents=True, exist_ok=True)

        (rpt_dir / "definition.pbir").write_text(json.dumps({
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definitionProperties/2.0.0/schema.json",
            "version": "4.0",
            "datasetReference": {
                "byPath": {"path": f"../{state.safe_name}.SemanticModel"}
            },
        }, indent=2), encoding="utf-8")

        (rpt_dir / "report.json").write_text(
            build_report_json(spec, source_table), encoding="utf-8"
        )
        state.rpt_dir = rpt_dir
        return json.dumps({
            "status": "ok",
            "path":   str(rpt_dir),
            "pages":  [p.get("displayName", "?") for p in pages],
        })

    # ── deploy_report ─────────────────────────────────────────────────────────
    if name == "deploy_report":
        if not state.rpt_dir:
            return json.dumps({"error": "call write_report_spec first"})
        result = json.loads(_m.deploy_report(
            workspace_id=state.workspace_id,
            display_name=inputs["display_name"],
            report_folder=str(state.rpt_dir),
            semantic_model_id=inputs["semantic_model_id"],
        ))
        if "id" in result:
            state.report_id = result["id"]
        return json.dumps(result)

    # ── export_report_page ────────────────────────────────────────────────────
    if name == "export_report_page":
        out = str(state.output_dir / "preview.png")
        return _m.export_report_page(
            workspace_id=state.workspace_id,
            report_id=inputs["report_id"],
            page_name=inputs["page_name"],
            output_path=out,
        )

    # ── finish ────────────────────────────────────────────────────────────────
    if name == "finish":
        sm_id  = inputs.get("semantic_model_id") or state.sm_id
        rpt_id = inputs.get("report_id")          or state.report_id
        print(f"\n{'='*60}")
        print(f"  ✓  {inputs['summary']}")
        print(f"  Semantic Model : {sm_id}")
        print(f"  Report         : {rpt_id}")
        print(f"{'='*60}")
        return json.dumps({"status": "done", "semantic_model_id": sm_id, "report_id": rpt_id})

    return json.dumps({"error": f"unknown tool: {name}"})


# ── Agentic loop ──────────────────────────────────────────────────────────────

def run_agent(canonical: dict, state: _State) -> None:
    """Run the Claude tool-use loop until the agent calls finish or we hit max turns."""
    client   = anthropic.Anthropic()
    MAX_TURNS = 40

    user_msg = f"""
Here is the canonical report model to migrate to Power BI:

```json
{json.dumps(canonical, indent=2)}
```

Fabric workspace ID: {state.workspace_id}

Deploy this as a working Semantic Model + Report on Fabric.
Follow the workflow: write SM → deploy → refresh → DAX verify → write report → deploy → export PNG → finish.
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

        # Collect text output
        for block in response.content:
            if hasattr(block, "text") and block.text.strip():
                print(f"  [Claude] {block.text.strip()[:200]}")

        messages.append({"role": "assistant", "content": response.content})

        # Done?
        if response.stop_reason == "end_turn":
            print("  Agent finished (end_turn).")
            break

        # Execute tool calls
        if response.stop_reason == "tool_use":
            tool_results = []
            done = False

            for block in response.content:
                if block.type != "tool_use":
                    continue
                result = _execute(block.name, block.input, state)
                print(f"      ← {result[:120]}")
                tool_results.append({
                    "type":        "tool_result",
                    "tool_use_id": block.id,
                    "content":     result,
                })
                if block.name == "finish":
                    done = True

            messages.append({"role": "user", "content": tool_results})
            if done:
                break
        else:
            print(f"  Unexpected stop_reason: {response.stop_reason}")
            break
    else:
        print(f"  WARNING: reached MAX_TURNS ({MAX_TURNS}) without finish.")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Agent 3 — Power BI Design Agent")
    parser.add_argument("--report-id",   default="cbf97b0a-457d-4b4f-8913-547e0cdf390c")
    parser.add_argument("--input-dir",   default="")
    parser.add_argument("--workspace-id", default=os.environ.get("FABRIC_WORKSPACE_ID", ""))
    args = parser.parse_args()

    workspace_id = args.workspace_id
    if not workspace_id:
        sys.exit("ERROR: set FABRIC_WORKSPACE_ID or pass --workspace-id")

    input_dir      = Path(args.input_dir or f"docs/{args.report_id}")
    canonical_path = input_dir / "canonical_model.json"
    if not canonical_path.exists():
        sys.exit(f"ERROR: {canonical_path} not found. Run agent2_canonical.py first.")

    canonical    = json.loads(canonical_path.read_text(encoding="utf-8"))
    report_name  = canonical.get("report", {}).get("name", "Report")
    safe_name    = report_name.replace(" ", "_")

    output_dir = Path(OUTPUT_BASE) / safe_name
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n{'='*60}")
    print(f"  Agent 3 — Power BI Design Agent (v2)")
    print(f"  Report    : {report_name}")
    print(f"  Workspace : {workspace_id}")
    print(f"  Input     : {canonical_path}")
    print(f"  Output    : {output_dir}/")
    print(f"{'='*60}")

    # Authenticate once
    auth = json.loads(_m.authenticate())
    if auth.get("fabric") != "ok":
        sys.exit(f"ERROR: Fabric auth failed: {auth}")
    print(f"  Auth: fabric={auth['fabric']}  storage={auth['storage']}\n")

    state = _State(report_name, workspace_id, output_dir)
    run_agent(canonical, state)


if __name__ == "__main__":
    main()
