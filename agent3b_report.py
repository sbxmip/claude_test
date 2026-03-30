#!/usr/bin/env python3
"""
Agent 3b — Report Design Agent
================================
Part of the SAS Visual Analytics → Power BI migration pipeline.

Reads:
  • manifest.json   (written by Agent 3a — contains semantic_model_id,
                     source_tables, deployed_measures, sql_endpoint)
  • canonical_model.json  (Agent 2 output — pages & visuals)

Then:
  1. Reads the canonical pages and visual specs
  2. Writes a PBIR-Legacy report spec (pages + visuals)
  3. Deploys the report to Fabric linked to the semantic model
  4. Exports the first page as PNG to verify rendering
  5. Writes a final summary

Usage:
    python agent3b_report.py [--report-id UUID] [--input-dir PATH]

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


# ── PBIR-Legacy builder ───────────────────────────────────────────────────────

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
    "slicer":                         ("Field",    None),
}

_AGG = {"sum": 0, "avg": 1, "count": 2, "min": 3, "max": 4}


def _build_visual_config(vis: dict, source_table: str) -> dict:
    """
    Convert one visual spec entry into a PBIR-Legacy singleVisual config dict.

    vis fields:
        type         visualType string (e.g. "columnChart")
        dim_col      dimension/category column name (optional for card)
        measure_col  raw column to aggregate (for inline Sum)
        measure_name named DAX measure (alternative to measure_col)
        agg          aggregation name: "sum"|"avg"|"count"|"min"|"max" (default "sum")
        position     {x, y, width, height, z, tabOrder}
    """
    vtype    = vis["type"]
    dim_col  = vis.get("dim_col", "")
    msr_col  = vis.get("measure_col", "")
    msr_name = vis.get("measure_name", "")
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
    role = val_role or cat_role   # card/slicer: val_role is None, use cat_role

    if msr_name:
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
          "name": "<hex20>",
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
            "useNewFilterPaneExperience":       True,
            "allowChangeFilterTypes":           True,
            "useStylableVisualContainerHeader": True,
            "queryLimitOption":                 6,
            "useEnhancedTooltips":              True,
            "exportDataMode":                   1,
            "useDefaultAggregateDisplayName":   True,
        },
    }

    sections = []
    for i, page in enumerate(spec.get("pages", [])):
        page_name  = page.get("name") or make_id()
        containers = []
        for vis in page.get("visuals", []):
            vc  = _build_visual_config(vis, source_table)
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
        "config":             json.dumps(report_config),
        "layoutOptimization": 0,
        "resourcePackages":   [],
        "sections":           sections,
    }, indent=2)


# ── Runtime state ─────────────────────────────────────────────────────────────

class _State:
    def __init__(self, canonical: dict, workspace_id: str,
                 output_dir: Path, manifest: dict):
        self.canonical    = canonical
        self.workspace_id = workspace_id
        self.output_dir   = output_dir
        self.manifest     = manifest

        self.report_name = canonical.get("report", {}).get("name", "Report")
        self.safe_name   = self.report_name.replace(" ", "_")

        # set by write_report_spec
        self.rpt_dir:        Path | None = None
        self.first_page_name: str        = "ReportSection"

        # set by deploy_report
        self.report_id: str = ""


# ── Tool executor ─────────────────────────────────────────────────────────────

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

    # ── read_manifest ─────────────────────────────────────────────────────────
    if name == "read_manifest":
        return json.dumps(state.manifest)

    # ── write_report_spec ─────────────────────────────────────────────────────
    if name == "write_report_spec":
        spec         = inputs["spec"]
        source_table = inputs["source_table"]

        pages = spec.get("pages", [])
        if pages:
            state.first_page_name = pages[0].get("name") or state.first_page_name

        sm_name  = state.manifest.get("semantic_model_name", state.safe_name)
        rpt_dir  = state.output_dir / f"{state.safe_name}.Report"
        rpt_dir.mkdir(parents=True, exist_ok=True)

        (rpt_dir / "definition.pbir").write_text(json.dumps({
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definitionProperties/2.0.0/schema.json",
            "version": "4.0",
            "datasetReference": {
                "byPath": {"path": f"../{sm_name}.SemanticModel"}
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
            workspace_id      = state.workspace_id,
            display_name      = inputs["display_name"],
            report_folder     = str(state.rpt_dir),
            semantic_model_id = inputs["semantic_model_id"],
        ))
        if "id" in result:
            state.report_id = result["id"]
        return json.dumps(result)

    # ── export_report_page ────────────────────────────────────────────────────
    if name == "export_report_page":
        out = str(state.output_dir / "preview.png")
        return _m.export_report_page(
            workspace_id = state.workspace_id,
            report_id    = inputs["report_id"],
            page_name    = inputs["page_name"],
            output_path  = out,
        )

    # ── finish ────────────────────────────────────────────────────────────────
    if name == "finish":
        rpt_id = inputs.get("report_id") or state.report_id
        sm_id  = state.manifest.get("semantic_model_id", "")
        print(f"\n{'='*60}")
        print(f"  ✓  {inputs['summary']}")
        print(f"  Semantic Model : {sm_id}")
        print(f"  Report         : {rpt_id}")
        print(f"{'='*60}")
        return json.dumps({"status": "done", "semantic_model_id": sm_id,
                           "report_id": rpt_id})

    return json.dumps({"error": f"unknown tool: {name}"})


# ── System prompt ─────────────────────────────────────────────────────────────

SYSTEM = """\
You are a Power BI Report Design agent. Your job is to read the canonical report \
model (from Agent 2) and the Semantic Model manifest (from Agent 3a) and deliver \
a working PBIR-Legacy report deployed on Microsoft Fabric.

WORKFLOW — call tools in this order:
  1. read_manifest()                  — get semantic_model_id, source_tables,
                                        deployed_measures, sql_endpoint
  2. read_canonical_model("overview") — get report name, page list, source names
  3. For each page in canonical pages[]:
       read_canonical_model("page", page_id=<id>)  — get visuals for that page
  4. read_canonical_model("source", name=<source_name>) — get dim + measure details
     (repeat per source to resolve dim_col for each visual)
  5. write_report_spec(spec, source_table)
  6. deploy_report(display_name, semantic_model_id)
  7. export_report_page(report_id, page_name)
  8. finish(summary, report_id)

If a tool returns an error: analyse it, fix the spec, and retry that step.

════════════════════════════════════════════════════════════
READING THE MANIFEST (from Agent 3a)
════════════════════════════════════════════════════════════
read_manifest returns:
  semantic_model_id    → use in deploy_report
  semantic_model_name  → name of the deployed Semantic Model
  workspace_id         → Fabric workspace
  lakehouse_id         → Fabric Lakehouse id
  sql_endpoint         → SQL Analytics Endpoint connection string
  sql_database         → Lakehouse name (used as database)
  source_tables        → {source_name: table_name_in_fabric}
                         e.g. {"rand_retaildemo": "rand_retaildemo_data"}
  deployed_measures    → list of DAX measure display labels created in TMDL
                         e.g. ["Total Sales", "Profit Margin", "Return Rate"]

════════════════════════════════════════════════════════════
READING THE CANONICAL MODEL
════════════════════════════════════════════════════════════
The canonical model has this structure:

semantic_model:
  sources[]       → {name, label, server, library, table}
  dimensions[]    → {name, label, expr, source, type}
                    expr = actual column name in the source table
  measures[]      → {name, label, agg, expr, source, format}
                    base aggregations: expr = column, agg = sum/average/count/min/max
  metrics[]       → {name, label, type, type_params, format}
                    type=simple: type_params.measure → measures[].name
                    type=ratio:  type_params.numerator + type_params.denominator
                    type=derived: type_params.expr → pseudo-math
  parameters[]    → {name, label, data_type, default, range}

pages[]:
  id, display_name
  visuals[]:
    visual_type   → canonical type (see mapping below)
    metrics[]     → list of metric names → resolve via semantic_model.metrics
    dimensions[]  → list of dimension names → resolve via semantic_model.dimensions
    spec          → Vega-Lite spec (reference only, do not use directly)

RESOLVING VISUALS:
  For each dimension name in visual.dimensions[]:
    → look up semantic_model.dimensions where name matches
    → dim_col = dimension.expr   (actual column name)
    → source  = dimension.source (which source table)

  For each metric name in visual.metrics[]:
    → look up semantic_model.metrics where name matches
    → if the metric label is in manifest.deployed_measures:
        use measure_name = that label   ← PREFERRED
    → else if type=simple: use measure_col = measures[type_params.measure].expr
                               agg        = measures[type_params.measure].agg
    → for ratio/derived: always use measure_name (these have named DAX measures)

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
map             → tableEx   (no native geo map; table with geo dim + metric)
text            → SKIP      (static labels — omit entirely)

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
          "dim_col": "Region",
          "measure_name": "Total Sales",
          "position": {"x": 10, "y": 10, "width": 600, "height": 380}
        },
        {
          "type": "card",
          "measure_name": "Profit Margin",
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
  • source_table = the source label from semantic_model.sources (e.g. "rand_retaildemo")
    This is the label — NOT the fabric table_name from manifest.source_tables.
    The prototypeQuery uses the Semantic Model table name which is the source label.
  • For visuals with deployed measure names → use measure_name (NOT measure_col)
  • For inline aggregations only → use measure_col + agg
  • dim_col = dimension.expr (actual column name, not the dimension's name)
  • Each page name must be a unique hex20 string (e.g. uuid4().hex[:20])
  • Skip text/static visuals entirely
  • Map each page from canonical pages[]
  • Canvas: 1280×720 px, ~10 px gap between visuals
  • Typical sizes:
      card:    280×120  |  bar/line chart: 580×340
      table:   780×300  |  slicer:         200×120
      treemap: 580×340  |  scatter:        500×380
  • Do not stack visuals — tile them across x/y without overlap
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
        "name": "read_manifest",
        "description": (
            "Read the manifest written by Agent 3a. "
            "Returns semantic_model_id, source_tables, deployed_measures, "
            "sql_endpoint, sql_database, lakehouse_id."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    {
        "name": "write_report_spec",
        "description": (
            "Write a structured report spec. "
            "Python converts this to a PBIR-Legacy report.json — "
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
                    "description": (
                        "Primary source label from semantic_model.sources "
                        "(used for prototypeQuery From clause)."
                    ),
                },
            },
            "required": ["spec", "source_table"],
        },
    },
    {
        "name": "deploy_report",
        "description": (
            "Deploy the written PBIR-Legacy report to Fabric, linked to the semantic model. "
            "Returns {id, displayName, created} or {error}."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "display_name":      {"type": "string"},
                "semantic_model_id": {"type": "string"},
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
                "report_id": {"type": "string"},
                "page_name": {
                    "type": "string",
                    "description": "Hex20 page name from write_report_spec (NOT displayName)",
                },
            },
            "required": ["report_id", "page_name"],
        },
    },
    {
        "name": "finish",
        "description": "Signal completion with a human-readable summary.",
        "input_schema": {
            "type": "object",
            "properties": {
                "summary":   {"type": "string"},
                "report_id": {"type": "string"},
            },
            "required": ["summary"],
        },
    },
]


# ── Agentic loop ──────────────────────────────────────────────────────────────

def run_agent(state: _State) -> None:
    client    = anthropic.Anthropic()
    MAX_TURNS = 40

    user_msg = f"""\
Migrate the following report to Power BI on Fabric.

Report name : {state.report_name}
Workspace   : {state.workspace_id}

Agent 3a has already deployed the Semantic Model and test data.
Use read_manifest to get the semantic_model_id and deployed_measures,
then read the canonical model pages, design the report spec, deploy it, \
export a PNG, and call finish.
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
    parser = argparse.ArgumentParser(description="Agent 3b — Report Design Agent")
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

    manifest_path = output_dir / "manifest.json"
    if not manifest_path.exists():
        sys.exit(
            f"ERROR: {manifest_path} not found. "
            "Run agent3a_semantic_model.py first to generate the manifest."
        )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    workspace_id = args.workspace_id or manifest.get("workspace_id", "")
    if not workspace_id:
        sys.exit("ERROR: set FABRIC_WORKSPACE_ID or pass --workspace-id")

    print(f"\n{'='*60}")
    print(f"  Agent 3b — Report Design Agent")
    print(f"  Report          : {report_name}")
    print(f"  Workspace       : {workspace_id}")
    print(f"  Semantic Model  : {manifest.get('semantic_model_id', '?')}")
    print(f"  Input           : {canonical_path}")
    print(f"  Manifest        : {manifest_path}")
    print(f"  Output          : {output_dir}/")
    print(f"{'='*60}")

    auth = json.loads(_m.authenticate())
    if auth.get("fabric") != "ok":
        sys.exit(f"ERROR: Fabric auth failed: {auth}")
    print(f"  Auth: fabric={auth['fabric']}  storage={auth['storage']}\n")

    state = _State(canonical, workspace_id, output_dir, manifest)
    run_agent(state)


if __name__ == "__main__":
    main()
