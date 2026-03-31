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
        # inline the filter definitions referenced by this page's visuals
        filter_map = {f["id"]: f for f in canonical.get("filters", [])}
        referenced_ids = set()
        for vis in page.get("visuals", []):
            referenced_ids.update(vis.get("applied_filters", []))
        result = dict(page)
        result["filter_definitions"] = {
            fid: filter_map[fid] for fid in referenced_ids if fid in filter_map
        }
        return json.dumps(result)

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


def _build_azure_map_config(vis: dict, source_table: str) -> dict:
    """Build PBIR-Legacy singleVisual config for an azureMap visual."""
    lat_col     = vis.get("lat_col", "")
    lon_col     = vis.get("lon_col", "")
    msr_name    = vis.get("measure_name", "")
    tooltip_col = vis.get("tooltip_col", "")

    frm  = [{"Name": "c", "Entity": source_table, "Type": 0}]
    sels = []
    proj = {}

    def _col_sel(col):
        return {
            "Column": {
                "Expression": {"SourceRef": {"Source": "c"}},
                "Property": col,
            },
            "Name": f"{source_table}.{col}",
            "NativeReferenceName": col,
        }

    def _msr_sel(msr):
        return {
            "Measure": {
                "Expression": {"SourceRef": {"Source": "c"}},
                "Property": msr,
            },
            "Name": f"{source_table}.{msr}",
            "NativeReferenceName": msr,
        }

    if lat_col:
        proj["Latitude"]  = [{"queryRef": f"{source_table}.{lat_col}"}]
        sels.append(_col_sel(lat_col))
    if lon_col:
        proj["Longitude"] = [{"queryRef": f"{source_table}.{lon_col}"}]
        sels.append(_col_sel(lon_col))
    if msr_name:
        proj["Size"]  = [{"queryRef": f"{source_table}.{msr_name}"}]
        proj["Color"] = [{"queryRef": f"{source_table}.{msr_name}"}]
        sels.append(_msr_sel(msr_name))
    if tooltip_col:
        proj["Tooltip"] = [{"queryRef": f"{source_table}.{tooltip_col}"}]
        sels.append(_col_sel(tooltip_col))

    pos = vis.get("position", {})
    az_proto = {"Version": 2, "From": frm, "Select": sels}
    az_where = _build_where_clauses(vis.get("filters", []), frm)
    if az_where:
        az_proto["Where"] = az_where
    return {
        "name": make_id(),
        "layouts": [{
            "id": 0,
            "position": {
                "x":        float(pos.get("x", 10)),
                "y":        float(pos.get("y", 10)),
                "z":        float(pos.get("z", 0)),
                "width":    float(pos.get("width", 780)),
                "height":   float(pos.get("height", 480)),
                "tabOrder": int(pos.get("tabOrder", 0)),
            },
        }],
        "singleVisual": {
            "visualType": "azureMap",
            "projections": proj,
            "prototypeQuery": az_proto,
            "drillFilterOtherVisuals": True,
        },
    }


def _build_visual_config(vis: dict, source_table: str) -> dict:
    """
    Convert one visual spec entry into a PBIR-Legacy singleVisual config dict.

    vis fields:
        type         visualType string (e.g. "columnChart")
        dim_col      dimension/category column name (optional for card)
        hierarchy    hierarchy name (e.g. "Merchandise Hierarchy") — enables drill-down;
                     dim_col is still used as the top level to show
        measure_col  raw column to aggregate (for inline Sum)
        measure_name named DAX measure (alternative to measure_col)
        agg          aggregation name: "sum"|"avg"|"count"|"min"|"max" (default "sum")
        position     {x, y, width, height, z, tabOrder}
    """
    vtype    = vis["type"]

    # azureMap has a dedicated builder (different projection roles)
    if vtype == "azureMap":
        return _build_azure_map_config(vis, source_table)

    dim_col   = vis.get("dim_col", "")
    hierarchy = vis.get("hierarchy", "")   # optional hierarchy name for drill-down
    msr_col   = vis.get("measure_col", "")
    msr_name  = vis.get("measure_name", "")
    agg_fn    = _AGG.get(vis.get("agg", "sum").lower(), 0)

    # Parameter table slicer: use the param table as the entity, not the main source table
    entity = vis.get("param_table", source_table)

    cat_role, val_role = _ROLE_MAP.get(vtype, ("Category", "Y"))

    projections: dict = {}
    selects: list     = []
    order_by: list    = []
    frm = [{"Name": "c", "Entity": entity, "Type": 0}]

    # ── dimension / category ──────────────────────────────────────────────────
    if dim_col:
        if hierarchy:
            # Hierarchy-aware projection: enables drill-down through hierarchy levels
            q_ref = f"{entity}.{hierarchy}.{dim_col}"
            projections[cat_role] = [{"queryRef": q_ref, "active": True}]
            selects.append({
                "HierarchyLevel": {
                    "Expression": {
                        "Hierarchy": {
                            "Expression": {"SourceRef": {"Source": "c"}},
                            "Hierarchy": hierarchy,
                        }
                    },
                    "Level": dim_col,
                },
                "Name": q_ref,
                "NativeReferenceName": dim_col,
            })
        else:
            q_ref = f"{entity}.{dim_col}"
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
        q_ref = f"{entity}.{msr_name}"
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
        q_ref   = f"{fn_name}({entity}.{msr_col})"
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
    # Embed filters as Where clauses — this is what Power BI actually uses
    # to filter the visual's DAX query (container `filters` only drives the pane)
    where = _build_where_clauses(vis.get("filters", []), frm)
    if where:
        proto["Where"] = where

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


def _build_where_clauses(pbi_filters: list, frm: list) -> list:
    """
    Convert PBI basic/advanced filter dicts into prototypeQuery Where clauses.
    This is what actually filters the visual's DAX query — the container `filters`
    field only drives the filter pane display, not query execution.
    """
    table_to_alias = {f["Entity"]: f["Name"] for f in frm}
    where_clauses = []

    for f in pbi_filters:
        schema   = f.get("$schema", "")
        target   = f.get("target", {})
        table    = target.get("table", "")
        column   = target.get("column", "")
        alias    = table_to_alias.get(table, "c")
        operator = f.get("operator", "In")
        values   = f.get("values", [])

        col_expr = {
            "Column": {
                "Expression": {"SourceRef": {"Source": alias}},
                "Property":   column,
            }
        }

        if "basic" in schema and operator == "In" and values:
            in_cond = {
                "In": {
                    "Expressions": [col_expr],
                    "Values":      [[{"Literal": {"Value": f"'{v}'"}}] for v in values],
                }
            }
            where_clauses.append({"Condition": in_cond})

        elif "advanced" in schema:
            # Or-logic (include_nulls variant): (col In values) OR IsBlank(col)
            conditions = f.get("conditions", [])
            in_vals    = [c["value"] for c in conditions if c.get("operator") == "Is"]
            has_blank  = any(c.get("operator") == "IsBlank" for c in conditions)
            if in_vals:
                in_cond = {
                    "In": {
                        "Expressions": [col_expr],
                        "Values":      [[{"Literal": {"Value": f"'{v}'"}}] for v in in_vals],
                    }
                }
                if has_blank:
                    blank_cond = {
                        "Not": {
                            "Expression": {
                                "FunctionCall": {
                                    "Function":           37,   # IsBlank
                                    "FunctionParameters": [col_expr],
                                }
                            }
                        }
                    }
                    where_clauses.append({
                        "Condition": {
                            "Or": {"Left": in_cond, "Right": blank_cond}
                        }
                    })
                else:
                    where_clauses.append({"Condition": in_cond})

    return where_clauses


def _resolve_applied_filters(applied: list, filter_map: dict,
                              dim_map: dict, src_label_map: dict,
                              fallback_source_label: str) -> list:
    """
    Translate canonical applied_filters (list of filter IDs) into
    Power BI basic/advanced filter dicts for the visual container's `filters` field.
    """
    pbi_filters = []
    for fid in applied:
        fdef = filter_map.get(fid)
        if not fdef or fdef.get("type") not in ("static",):
            continue  # skip rank filters — not representable as PBI static filter
        dim     = dim_map.get(fdef.get("dimension", ""), {})
        column  = dim.get("expr",   fdef.get("dimension", ""))
        src_key = dim.get("source", "")
        table   = src_label_map.get(src_key, fallback_source_label)
        op      = fdef.get("operator", "in")
        values  = fdef.get("values", [])
        include_nulls = fdef.get("include_nulls", False)

        if op == "in" and values:
            if include_nulls:
                # advanced filter: (col In values) OR IsBlank
                pbi_filters.append({
                    "$schema": "https://powerbi.com/product/schema#advanced",
                    "target":          {"table": table, "column": column},
                    "filterType":      0,
                    "logicalOperator": "Or",
                    "conditions": [
                        *[{"operator": "Is", "value": v} for v in values],
                        {"operator": "IsBlank"},
                    ],
                })
            else:
                pbi_filters.append({
                    "$schema": "https://powerbi.com/product/schema#basic",
                    "target":                 {"table": table, "column": column},
                    "filterType":             1,
                    "operator":               "In",
                    "values":                 values,
                    "requireSingleSelection": False,
                })
        elif op == "is_not_null":
            pbi_filters.append({
                "$schema": "https://powerbi.com/product/schema#advanced",
                "target":          {"table": table, "column": column},
                "filterType":      0,
                "logicalOperator": "And",
                "conditions":      [{"operator": "IsNotBlank"}],
            })
    return pbi_filters


def build_report_json(pages: list, default_source_table: str) -> str:
    """
    Convert a list of page specs into a PBIR-Legacy report.json string.
    Each visual may carry its own source_table; falls back to default_source_table.
    Note: applied_filters must already be resolved to PBI format (see write_report_spec).
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
    for i, page in enumerate(pages):
        page_name  = page.get("name") or make_id()
        # use the page's own stored source_table, fall back to the global default
        page_default_source = page.get("_source_table", default_source_table)
        containers = []
        for vis in page.get("visuals", []):
            # per-visual source_table overrides the page default
            vis_source = vis.get("source_table", page_default_source)
            vc  = _build_visual_config(vis, vis_source)
            pos = vis.get("position", {})
            # visual-level filters (e.g. banner filters): list of PBI basic filter dicts
            vis_filters = vis.get("filters", [])
            containers.append({
                "config":  json.dumps(vc),
                "filters": json.dumps(vis_filters),
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

        # set by write_report_spec (accumulates pages one at a time)
        self.rpt_dir:        Path | None = None
        self.pages:          list        = []   # accumulated page specs
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
        page         = inputs["page"]           # single page dict
        source_table = inputs.get("source_table", "rand_retaildemo")

        # Resolve applied_filters on each visual → PBI filter dicts
        _filter_map   = {f["id"]: f for f in state.canonical.get("filters", [])}
        _sm           = state.canonical.get("semantic_model", {})
        _dim_map      = {d["name"]: d for d in _sm.get("dimensions", [])}
        _src_lbl_map  = {s["name"]: s["label"] for s in _sm.get("sources", [])}
        for vis in page.get("visuals", []):
            applied = vis.get("applied_filters", [])
            if applied and not vis.get("filters"):
                vis["filters"] = _resolve_applied_filters(
                    applied, _filter_map, _dim_map, _src_lbl_map, source_table
                )

        # upsert page into state.pages by name
        page_name = page.get("name") or make_id()
        page["name"] = page_name
        # store the page's own default source_table so it survives later pages
        page["_source_table"] = source_table
        state.pages = [p for p in state.pages if p.get("name") != page_name]
        state.pages.append(page)

        # track first page name for export
        if len(state.pages) == 1:
            state.first_page_name = page_name

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

        # rewrite report.json from ALL accumulated pages
        # each page carries its own _source_table — pass "" as fallback default
        (rpt_dir / "report.json").write_text(
            build_report_json(state.pages, ""), encoding="utf-8"
        )
        state.rpt_dir = rpt_dir
        return json.dumps({
            "status":        "ok",
            "path":          str(rpt_dir),
            "page_name":     page_name,
            "page_display":  page.get("displayName", "?"),
            "total_pages":   len(state.pages),
            "all_pages":     [p.get("displayName", "?") for p in state.pages],
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
        import base64 as _b64
        safe_page = inputs["page_name"][:20]
        out = str(state.output_dir / f"preview_{safe_page}.png")
        result_str = _m.export_report_page(
            workspace_id = state.workspace_id,
            report_id    = inputs["report_id"],
            page_name    = inputs["page_name"],
            output_path  = out,
        )
        result = json.loads(result_str)
        if result.get("status") == "ok":
            try:
                img_data = _b64.b64encode(Path(out).read_bytes()).decode()
                return [
                    {"type": "text",  "text": result_str},
                    {"type": "image", "source": {
                        "type":       "base64",
                        "media_type": "image/png",
                        "data":       img_data,
                    }},
                ]
            except Exception:
                pass  # fall through to plain text if image read fails
        return result_str

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

WORKFLOW — PAGE-BY-PAGE approach (STRICT ORDER):

  SETUP (once):
    1. read_manifest()                   — get semantic_model_id, source_tables,
                                           deployed_measures, sql_endpoint
    2. read_canonical_model("overview")  — get report name, page list, parameters,
                                           hierarchies, source names
    3. read_canonical_model("source", name=<primary_source>) — resolve dim/measure details

  FOR EACH PAGE in canonical pages[] (one at a time):
    a. read_canonical_model("page", page_id=<id>)
         — get visuals[], dimensions[], metrics[] for this page only
    b. write_report_spec(page={single page spec}, source_table=<primary source for this page>)
         — writes ONE page; the tool accumulates pages in the report automatically
         — use a unique hex20 string as the page "name"
    c. deploy_report(display_name, semantic_model_id)
         — re-deploys the whole report (all pages written so far)
         — on first page: use display_name = "<ReportName>"
         — on subsequent pages: use the SAME display_name (update in place)
    d. export_report_page(report_id, page_name)
         — page_name = the hex20 name you gave in write_report_spec
         — visually inspect the PNG:
             • Are all visuals rendering with data (not blank/empty)?
             • Are there any error messages or broken visual indicators?
             • Does the layout look reasonable (no overlaps)?
             • For maps: is the Azure Map rendering with bubbles/color?
    e. If issues found: fix write_report_spec for this page (same name to overwrite),
       re-deploy, re-export (max 2 fix attempts per page)
    f. Move to next page only after this page passes QA

  FINISH (after all pages):
    finish(summary, report_id)
      — summary must include the visual QA result for each page (Pass/Fail + issues)

If a tool returns an error: analyse it, fix the input, and retry that step.
NEVER skip the export step — every page must be visually inspected.

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
map             → azureMap  (native Power BI geo map using lat/long columns)
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
        },
        {
          "type": "card",
          "measure_name": "Marketing Pct of Sales",
          "applied_filters": ["filter_storechain_grand"],
          "position": {"x": 620, "y": 130, "width": 280, "height": 120}
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

azureMap visual spec (use instead of the generic format for map visuals):
  {
    "type": "azureMap",
    "lat_col":     "<latitude column expr, e.g. State_Lat>",
    "lon_col":     "<longitude column expr, e.g. State_Long>",
    "measure_name": "<deployed DAX measure label, e.g. Adjust ROI>",
    "tooltip_col": "<dimension expr for tooltip label, e.g. State>",
    "position":    {"x": 10, "y": 10, "width": 780, "height": 480}
  }
  Rules:
  - lat_col / lon_col must be the dimension.expr values (actual column names)
    from semantic_model.dimensions where type="geo" and geo_role="latitude"/"longitude"
  - Use State_Lat/State_Long for state-level maps, City_Lat/City_Long for city maps
  - measure_name must be a label from manifest.deployed_measures
  - azureMap is sized wider: typical 780×480 or full-width 1260×500

════════════════════════════════════════════════════════════
FILTERS (first-class citizens — ALWAYS apply them)
════════════════════════════════════════════════════════════
The canonical model has a top-level filters[] list with fully-defined filter objects.
Each canonical visual has an applied_filters[] list of filter IDs.

RULE: For EVERY visual that has applied_filters in the canonical page spec, you MUST
copy those same filter IDs into the visual's "applied_filters" field in your spec.
The framework resolves them to Power BI format automatically — do NOT construct
raw Power BI filter JSON yourself.

Example — Store Banner Dashboard KPI card for GRAND:
  canonical visual: applied_filters: ["filter_storechain_grand"]
  your spec:
  {
    "type": "card",
    "measure_name": "Marketing Pct of Sales",
    "applied_filters": ["filter_storechain_grand"],
    "position": {...}
  }

The framework looks up filter_storechain_grand in canonical filters[], resolves
the dimension column and values, and produces the correct PBI filter JSON.

Rules:
  • ALWAYS check applied_filters on each canonical visual and copy them to your spec
  • For rank filters (e.g. rank_top5_sales_grand) — these are Top-N filters.
    They cannot be represented as PBI static filters, so skip them. Instead use
    the visual's built-in sort+limit (use order_by descending on the measure).
  • Never omit filters — missing filters cause all banner visuals to show the same total

════════════════════════════════════════════════════════════
WHAT-IF PARAMETER SLICERS
════════════════════════════════════════════════════════════
The canonical model has parameters[] — these are what-if simulation parameters (e.g. Sales
Change, Cost Change). Each parameter has its own dedicated table in the semantic model
(deployed by Agent 3a) with a SELECTEDVALUE measure.

For EACH parameter, add a slicer visual on every page that uses a metric affected by
that parameter (check parameters[].affects_metrics). Typically place parameter slicers
near the top or right edge of the page.

Parameter slicer visual spec:
  {
    "type": "slicer",
    "dim_col": "<parameters[].label>",     ← the parameter table column name = parameter label
    "param_table": "<parameters[].label>", ← set this flag so the builder uses the param table
    "position": {"x": ..., "y": ..., "width": 220, "height": 60}
  }

The dim_col for a parameter slicer is the parameter's label (e.g. "Sales Change", "Cost Change")
because the parameter table column is named identically to the table.

════════════════════════════════════════════════════════════
HIERARCHY AWARENESS
════════════════════════════════════════════════════════════
The canonical model has hierarchies[] (e.g. Merchandise Hierarchy: Department→Class).
These are deployed as TMDL hierarchy blocks by Agent 3a.

When a canonical visual has dimensions[] that include hierarchy levels (check the hierarchy
definitions returned by read_canonical_model("overview")), use the "hierarchy" field in the
visual spec to enable drill-down:

  {
    "type": "clusteredBarChart",
    "dim_col": "Department",          ← top-level of the hierarchy (coarsest grain)
    "hierarchy": "Merchandise Hierarchy",  ← exact hierarchy label from canonical hierarchies[]
    "measure_name": "Total Sales",
    "position": {...}
  }

This generates a HierarchyLevel projection in the PBIR query so Power BI enables
drill-down from Department → Class automatically.

Rules:
  • Only use "hierarchy" when the visual's dimensions[] reference multiple levels of
    the same hierarchy (e.g. department + class together)
  • dim_col = the top-level hierarchy level's dimension.expr (e.g. "Department")
  • hierarchy = exact hierarchy label from canonical semantic_model.hierarchies[].label
  • For single-level dimension refs (e.g. only Region, no sub-level), do NOT set hierarchy
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
            "Write ONE page of the report spec. Call once per page. "
            "Pages accumulate automatically — the report grows with each call. "
            "Use the same page name to overwrite/fix a page. "
            "Python converts this to PBIR-Legacy report.json — no manual JSON needed."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "page": {
                    "type": "object",
                    "description": (
                        "Single page spec: "
                        "{name: <hex20>, displayName: <str>, visuals: [{type, dim_col, "
                        "measure_name, position, source_table?, param_table?, "
                        "applied_filters?: [filter_id strings from canonical filters[]]}]}"
                    ),
                },
                "source_table": {
                    "type": "string",
                    "description": (
                        "Default source label for this page's visuals "
                        "(from semantic_model.sources). "
                        "Individual visuals can override with their own source_table field."
                    ),
                },
            },
            "required": ["page", "source_table"],
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
            "Export a report page as PNG. Returns the rendered image so you can visually "
            "inspect it — check for blank visuals, error states, layout issues, and data presence. "
            "Call once per page. Returns {status, bytes, path} + the PNG image."
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
        "description": (
            "Signal completion. summary must include: report URL/id, "
            "and a visual QA result per page (Pass/Fail + any issues observed in the PNG)."
        ),
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
Migrate the following report to Power BI on Fabric using the PAGE-BY-PAGE workflow.

Report name : {state.report_name}
Workspace   : {state.workspace_id}

Agent 3a has already deployed the Semantic Model and test data.

Follow the workflow STRICTLY:
1. read_manifest + read_canonical_model("overview") + read_canonical_model("source", ...)
2. For EACH canonical page, one at a time:
   a. read_canonical_model("page", page_id=...)
   b. write_report_spec(page={{single page}}, source_table=...)
   c. deploy_report(...)
   d. export_report_page(...) — inspect the PNG
   e. Fix if needed (rewrite same page, redeploy, re-export), then move to next page
3. finish(...) after all pages pass QA

Do NOT batch all pages into a single write_report_spec call.
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
                # result may be a string or a list (text + image for export_report_page)
                preview = result[0]["text"][:200] if isinstance(result, list) else result[:200]
                print(f"      ← {preview}")
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
