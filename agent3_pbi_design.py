#!/usr/bin/env python3
"""
Agent 3 — Power BI Design Agent
=================================
Part of the SAS Visual Analytics → Power BI migration pipeline.

Reads canonical_model.json (Agent 2 output) and produces a complete .pbip
folder structure ready to open in Power BI Desktop or deploy via Fabric API:

    pbi/<ReportName>.pbip/
      <ReportName>.SemanticModel/
        definition/
          database.tmdl
          model.tmdl
          tables/<TableName>.tmdl      ← columns + DAX measures
          tables/<ParamName>.tmdl      ← what-if parameter tables
        definition.pbism
        .platform
      <ReportName>.Report/
        definition/
          report.json
          version.json
          pages/
            pages.json
            <pageId>/
              page.json
              visuals/<visualId>/
                visual.json
        definition.pbir
        .platform
      <ReportName>.pbip

Usage:
    python agent3_pbi_design.py [--report-id UUID] [--input-dir PATH]

Environment:
    ANTHROPIC_API_KEY
"""

import argparse
import json
import os
import sys
import uuid
from pathlib import Path

import anthropic

MODEL = "claude-opus-4-6"
MAX_TOKENS = 16000
OUTPUT_BASE = "pbi"

# ── ID helpers ────────────────────────────────────────────────────────────────

def make_id() -> str:
    return uuid.uuid4().hex[:20]

def make_uuid() -> str:
    return str(uuid.uuid4())

# ── System prompts ────────────────────────────────────────────────────────────

TMDL_SYSTEM = """\
You are a Power BI semantic model expert generating TMDL (Tabular Model Definition Language) files.

OUTPUT FORMAT
Return a single JSON object where keys are file paths and values are file contents (strings).
No explanation. No markdown outside the JSON fence. Example shape:
```json
{
  "definition/database.tmdl": "database ...",
  "definition/model.tmdl": "model Model\\n    culture: en-US",
  "definition/tables/Sales.tmdl": "table Sales\\n    ...",
  "definition.pbism": "{ ... }"
}
```

TMDL SYNTAX RULES
─────────────────
Indentation: 4 spaces per level (no tabs).
String values with spaces: wrap in single quotes → 'Net Sales'.

database.tmdl:
    database <ModelName>
        compatibilityLevel: 1605

model.tmdl:
    model Model
        culture: en-US
        discourageImplicitMeasures

    ref table <TableName>     ← one per table, including parameter tables

Table file (definition/tables/<TableName>.tmdl):
    table <TableName>
        column <ColName>
            dataType: int64 | decimal | string | dateTime | boolean
            sourceColumn: <CAS column name>
            summarizeBy: none | sum | average | count
            formatString: $ #,##0.00 | 0.00% | #,##0
            isHidden          ← for key columns not shown in report

        measure '<Measure Name>' = <DAX expression>
            formatString: $ #,##0 | 0.00% | #,##0.##

        partition '<TableName>-Partition' = m
            mode: import
            source =
                let
                    Source = SasViya.Dataset("<server>", "<library>", "<table>")
                in
                    Source

What-if parameter table:
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
                GENERATESERIES(<min>, <max>, <step>)

        annotation PBI_FormatHint = {"isGeneralNumber":true}

definition.pbism content:
    {
      "version": "4.0",
      "settings": {}
    }

DAX TRANSLATION RULES
─────────────────────
Canonical type → DAX pattern:

simple  (measure: "sales")          →  [Label] = SUM('Table'[col])
                                        or AVERAGE, COUNT, MIN, MAX as per agg

ratio   (num metric / den metric)   →  [Label] = DIVIDE([Num Metric], [Den Metric])

derived (expr with metrics + params) → translate pseudo-math:
    sum(x)           → SUM('Table'[col_for_x])
    average(x)       → AVERAGE('Table'[col_for_x])
    metric_name      → [Display Name of that metric]
    param_name       → [Param Label Value]   ← the SELECTEDVALUE measure
    IF cond THEN a ELSE b  → IF(cond, a, b)
    x IN {a,b,c}     → x IN {a,b,c}   (DAX IN syntax)
    x / y            → DIVIDE(x, y)

Format strings:
    currency    →  $ #,##0
    percentage  →  0.00%
    number      →  #,##0.##
    integer     →  #,##0
"""

PBIR_SYSTEM = """\
You are a Power BI report expert generating PBIR (Power BI Enhanced Report Format) files.

OUTPUT FORMAT
Return a single JSON object where keys are file paths and values are file contents.
Values that are JSON files must themselves be valid JSON (as a string).
No markdown outside the outer ```json fence.

FILE CONTENTS TO GENERATE
──────────────────────────

definition/version.json:
    {"version": "5.0"}

definition/report.json:
    {
      "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/report/2.0.0/schema.json",
      "themeCollection": {"baseTheme": {"name": "CY24SU06", "version": "5.60", "type": "SharedResources"}}
    }

definition/pages/pages.json:
    {
      "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/pagesMetadata/1.0.0/schema.json",
      "pageOrder": ["<pageId1>", "<pageId2>", ...],
      "activePageName": "<pageId1>"
    }

definition/pages/<pageId>/page.json:
    {
      "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/2.0.0/schema.json",
      "name": "<pageId>",
      "displayName": "<page display name>",
      "displayOption": "FitToWidth",
      "height": 720.0,
      "width": 1280.0
    }

definition/pages/<pageId>/visuals/<visualId>/visual.json:
    {
      "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.0.0/schema.json",
      "name": "<visualId>",
      "position": {"x": <float>, "y": <float>, "z": 1000.0, "height": <float>, "width": <float>},
      "visual": {
        "visualType": "<see mapping>",
        "query": { "queryState": { <roles> } }
      }
    }

VISUAL TYPE MAPPING  (canonical → Power BI)
─────────────────────────────────────────────
bar_chart                → clusteredColumnChart
line_chart               → lineChart
combo_chart              → lineClusteredColumnComboChart
kpi_card                 → card
treemap                  → treemap
map                      → map
scatter_plot             → scatterChart
table                    → tableEx
crosstab                 → pivotTable
filter_control           → slicer
text                     → textbox

QUERY STATE ROLES PER VISUAL TYPE
───────────────────────────────────
clusteredColumnChart / barChart:
    Category → dimension columns
    Y        → measure values
    Series   → (optional) legend dimension

lineChart:
    Axis     → dimension (x-axis)
    Y        → measure values
    Legend   → (optional) series dimension

lineClusteredColumnComboChart:
    Category → shared x-axis dimension
    Y        → bar measures (column series)
    Y2       → line measures (line series)

card:
    Values   → single measure

treemap:
    Group    → primary dimension
    Values   → measure
    Details  → (optional) sub-dimension

tableEx / pivotTable:
    Values   → all columns (dimensions + measures)
    Rows     → row dimensions (pivotTable only)
    Columns  → column dimensions (pivotTable only)

slicer:
    Values   → the field to filter on

FIELD REFERENCE SYNTAX
────────────────────────
Column:
    {"Column": {"Expression": {"SourceRef": {"Entity": "<TableName>"}}, "Property": "<ColDisplayName>"}}

Measure:
    {"Measure": {"Expression": {"SourceRef": {"Entity": "<TableName>"}}, "Property": "<MeasureDisplayName>"}}

Each projection:
    {"field": <field_ref>, "queryRef": "<TableName>.<FieldName>"}

LAYOUT RULES
─────────────
Page canvas: 1280 × 720 px. Lay out visuals in a sensible grid.
kpi_card:            width=280,  height=120
bar_chart:           width=580,  height=280
line_chart:          width=580,  height=280
combo_chart:         width=580,  height=280
treemap:             width=580,  height=280
map:                 width=580,  height=280
card:                width=200,  height=100
table / crosstab:    width=580,  height=280
slicer:              width=200,  height=100
text:                width=400,  height=60

Leave ~10px gaps between visuals. Start at x=10, y=10.
"""

# ── Claude call helper ────────────────────────────────────────────────────────

def call_claude(system: str, user_message: str, label: str) -> dict:
    """Call Claude, handle max_tokens continuation, extract JSON dict from response."""
    client = anthropic.Anthropic()
    messages = [{"role": "user", "content": user_message}]
    all_text = ""

    for iteration in range(8):
        response = client.messages.create(
            model=MODEL,
            max_tokens=MAX_TOKENS,
            system=system,
            messages=messages,
        )
        print(f"  [{label}] iter={iteration+1} stop={response.stop_reason} "
              f"in={response.usage.input_tokens} out={response.usage.output_tokens}")

        for block in response.content:
            if hasattr(block, "text"):
                all_text += block.text

        if response.stop_reason == "end_turn":
            break
        if response.stop_reason == "max_tokens":
            messages.append({"role": "assistant", "content": response.content})
            messages.append({"role": "user", "content": "Continue exactly where you left off."})
        else:
            break

    # Extract JSON
    if "```json" in all_text:
        start = all_text.index("```json") + 7
        end = all_text.rindex("```")
        raw = all_text[start:end].strip()
    else:
        raw = all_text.strip()

    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        debug = Path(f"debug_{label.replace(' ', '_')}.txt")
        debug.write_text(all_text)
        print(f"  [ERROR] JSON parse failed: {e}. Raw saved to {debug}")
        sys.exit(1)

# ── TMDL generation ───────────────────────────────────────────────────────────

def generate_tmdl(canonical: dict, report_name: str) -> dict:
    """Ask Claude to produce all TMDL files from the semantic_model section."""
    sm = canonical["semantic_model"]
    user_msg = f"""
Generate all TMDL files for the Power BI semantic model named "{report_name}".

## Semantic Model
```json
{json.dumps(sm, indent=2)}
```

## Field resolver (measure name → display label)
Build column names from dimension.label and measure/metric.label fields above.
Use dimension.expr as sourceColumn.

## Instructions
- One table file per source in sources[] — include all dimensions and measures for that source
- One table file per parameter in parameters[] — what-if pattern
- model.tmdl must have `ref table` for every table generated
- database.tmdl uses model name: {report_name.replace(" ", "_")}
- For the partition source, use placeholder:
    let Source = SasViya.Dataset("{sm['sources'][0].get('connection', {}).get('server', 'cas-server')}", \
"{sm['sources'][0].get('connection', {}).get('library', 'Samples')}", \
"{sm['sources'][0].get('connection', {}).get('table', 'TABLE')}") in Source
- For derived metrics that reference parameters, use the parameter's SELECTEDVALUE measure
- Translate all metric pseudo-formulas to valid DAX

Return the JSON dict of {{file_path: content}}.
"""
    print("[1/3] Generating TMDL (semantic model)...")
    return call_claude(TMDL_SYSTEM, user_msg, "TMDL")

# ── PBIR generation ───────────────────────────────────────────────────────────

def generate_pbir(canonical: dict, tmdl_files: dict, report_name: str, page_ids: dict, visual_ids: dict) -> dict:
    """Ask Claude to produce all PBIR files from the pages section."""

    # Extract measure names from TMDL for reference
    measure_names = []
    for path, content in tmdl_files.items():
        if "tables/" in path:
            for line in content.splitlines():
                stripped = line.strip()
                if stripped.startswith("measure "):
                    parts = stripped.split("=")[0].replace("measure", "").strip().strip("'")
                    measure_names.append(parts)

    user_msg = f"""
Generate all PBIR files for report "{report_name}".

## Pages and Visuals
```json
{json.dumps(canonical.get("pages", []), indent=2)}
```

## Filters
```json
{json.dumps(canonical.get("filters", []), indent=2)}
```

## Semantic Model — table names and measures available
Sources: {[s['label'] for s in canonical['semantic_model'].get('sources', [])]}
Measures in TMDL: {measure_names}
Dimensions: {[d['label'] for d in canonical['semantic_model'].get('dimensions', [])]}

## Pre-assigned IDs (use exactly these)
Page IDs: {json.dumps(page_ids)}
Visual IDs per page: {json.dumps(visual_ids)}

## Instructions
- Use the pre-assigned page and visual IDs above
- pages.json pageOrder must match the page order in the Pages section
- For each visual, reference the correct table name (use the source label from sources above)
- For measures, Entity = the table name where that measure is defined
- For dimensions/columns, Entity = the table name, Property = dimension label
- Omit "spec" from canonical model — instead use visualType + queryState
- Lay out visuals in a sensible grid within 1280×720 canvas
- For visuals with no query (text, filter_control with no data): omit queryState

Return the JSON dict of {{file_path: content}}.
All values must be valid JSON strings (escape inner quotes).
"""
    print("[2/3] Generating PBIR (report definition)...")
    return call_claude(PBIR_SYSTEM, user_msg, "PBIR")

# ── Write PBIP folder ─────────────────────────────────────────────────────────

def write_pbip(report_name: str, report_id: str,
               tmdl_files: dict, pbir_files: dict,
               output_dir: Path) -> Path:
    """Write all files into the .pbip directory structure."""

    safe_name = report_name.replace(" ", "_")
    pbip_root = output_dir / f"{safe_name}.pbip"
    sm_root   = pbip_root / f"{safe_name}.SemanticModel"
    rep_root  = pbip_root / f"{safe_name}.Report"

    # ── Semantic model files ──────────────────────────────────────────────────
    for rel_path, content in tmdl_files.items():
        full = sm_root / rel_path
        full.parent.mkdir(parents=True, exist_ok=True)
        full.write_text(content, encoding="utf-8")

    # definition.pbism (may be in tmdl_files or write default)
    pbism_path = sm_root / "definition.pbism"
    if not pbism_path.exists():
        pbism_path.write_text('{"version": "4.0", "settings": {}}', encoding="utf-8")

    # .platform for semantic model
    (sm_root / ".platform").write_text(json.dumps({
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/platform/platformProperties.json",
        "version": "2.0",
        "config": {"logicalId": make_uuid()},
        "metadata": {"type": "SemanticModel", "displayName": f"{safe_name}"}
    }, indent=2), encoding="utf-8")

    # ── Report files ──────────────────────────────────────────────────────────
    for rel_path, content in pbir_files.items():
        full = rep_root / rel_path
        full.parent.mkdir(parents=True, exist_ok=True)
        # Content may arrive as dict (Claude returned parsed JSON) or string
        if isinstance(content, dict):
            content = json.dumps(content, indent=2)
        full.write_text(content, encoding="utf-8")

    # definition.pbir — local byPath reference
    (rep_root / "definition.pbir").write_text(json.dumps({
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definitionProperties/2.0.0/schema.json",
        "version": "4.0",
        "datasetReference": {
            "byPath": {"path": f"../{safe_name}.SemanticModel"}
        }
    }, indent=2), encoding="utf-8")

    # .platform for report
    (rep_root / ".platform").write_text(json.dumps({
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/platform/platformProperties.json",
        "version": "2.0",
        "config": {"logicalId": make_uuid()},
        "metadata": {"type": "Report", "displayName": safe_name}
    }, indent=2), encoding="utf-8")

    # ── Root .pbip file ───────────────────────────────────────────────────────
    (pbip_root / f"{safe_name}.pbip").write_text(json.dumps({
        "version": "1.0",
        "artifacts": [{"report": {"path": f"{safe_name}.Report"}}],
        "settings": {"enableTmdlView": True}
    }, indent=2), encoding="utf-8")

    return pbip_root

# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Agent 3 — Power BI Design Agent")
    parser.add_argument("--report-id", default="cbf97b0a-457d-4b4f-8913-547e0cdf390c")
    parser.add_argument("--input-dir", default="")
    args = parser.parse_args()

    input_dir = Path(args.input_dir or f"docs/{args.report_id}")
    canonical_path = input_dir / "canonical_model.json"
    if not canonical_path.exists():
        sys.exit(f"ERROR: {canonical_path} not found. Run agent2_canonical.py first.")

    canonical = json.loads(canonical_path.read_text(encoding="utf-8"))
    report_name = canonical.get("report", {}).get("name", "Report")
    safe_name = report_name.replace(" ", "_")

    output_dir = Path(OUTPUT_BASE)
    output_dir.mkdir(exist_ok=True)

    print(f"\n{'='*60}")
    print(f"  Agent 3 — Power BI Design Agent")
    print(f"  Report : {report_name}")
    print(f"  Input  : {canonical_path}")
    print(f"  Output : {output_dir}/{safe_name}.pbip/")
    print(f"{'='*60}\n")

    # Pre-assign IDs so PBIR references match pages.json
    page_ids   = {p["id"]: make_id() for p in canonical.get("pages", [])}
    visual_ids = {
        p["id"]: {v["id"]: make_id() for v in p.get("visuals", [])}
        for p in canonical.get("pages", [])
    }

    # Pass 1: TMDL
    tmdl_files = generate_tmdl(canonical, report_name)
    print(f"      Generated {len(tmdl_files)} TMDL files: {list(tmdl_files.keys())}")

    # Pass 2: PBIR
    pbir_files = generate_pbir(canonical, tmdl_files, report_name, page_ids, visual_ids)
    print(f"      Generated {len(pbir_files)} PBIR files")

    # Write .pbip
    print("[3/3] Writing .pbip folder structure...")
    pbip_root = write_pbip(report_name, args.report_id,
                           tmdl_files, pbir_files, output_dir)

    # Summary
    all_files = sorted(pbip_root.rglob("*"))
    file_list = [str(f.relative_to(output_dir)) for f in all_files if f.is_file()]
    print(f"\n  ✓ PBIP written to: {pbip_root}")
    print(f"    {len(file_list)} files:")
    for f in file_list:
        print(f"      {f}")

if __name__ == "__main__":
    main()
