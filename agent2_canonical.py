#!/usr/bin/env python3
"""
Agent 2 — Canonical Model Builder
===================================
Part of the SAS Visual Analytics → Power BI migration pipeline.

Reads Agent 1 output (docs/<report-id>/) and produces a single
canonical_model.json using:
  - dbt Semantic Layer schema  for sources / dimensions / measures / metrics / parameters
  - Vega-Lite spec             for visual definitions (named data refs, no inline data)

Output: docs/<report-id>/canonical_model.json

Usage:
    python agent2_canonical.py [--input-dir PATH] [--report-id UUID]
"""

import argparse
import json
import os
import sys
from pathlib import Path

import anthropic

OUTPUT_FILE = "canonical_model.json"
MODEL = "claude-opus-4-6"
MAX_TOKENS = 16000

# ── Schema specification (given verbatim to the agent) ────────────────────────

CANONICAL_SCHEMA = """
The canonical_model.json you must produce has this exact top-level structure:

{
  "canonical_model_version": "1.0",
  "generated_by": "Agent 2 - SAS VA → Power BI Migration",
  "source_report_id": "<uuid>",

  "report": {
    "name": "...",
    "description": "...",
    "purpose": "...",          // infer from report content
    "created_by": "...",
    "original_tool": "SAS Visual Analytics"
  },

  "semantic_model": {          // ← dbt Semantic Layer schema

    "sources": [
      {
        "name": "<snake_case_identifier>",
        "label": "<human label>",
        "description": "...",
        "connection": { "server": "...", "library": "...", "table": "..." }
      }
    ],

    "entities": [              // join keys
      { "name": "...", "type": "primary|foreign|unique", "source": "...", "expr": "<column>" }
    ],

    "dimensions": [
      {
        "name": "<snake_case>",
        "label": "...",
        "type": "categorical | time",
        "source": "<source name>",
        "expr": "<CAS column name from xref>",
        // for time dimensions only:
        "time_granularity": "day | week | month | quarter | year"
      }
    ],

    "hierarchies": [
      {
        "name": "<snake_case>",
        "label": "...",
        "source": "...",
        "levels": ["<dimension_name>", ...]   // ordered from broadest to most specific
      }
    ],

    "measures": [              // raw aggregations — building blocks for metrics
      {
        "name": "<snake_case>",
        "label": "...",
        "description": "...",
        "agg": "sum | average | count | count_distinct | min | max",
        "source": "<source name>",
        "expr": "<CAS column name from xref>",
        "format": "currency | percentage | number | integer"
      }
    ],

    "metrics": [               // business calculations — dbt metric types
      // --- SIMPLE (wraps a single measure) ---
      {
        "name": "<snake_case>",
        "label": "...",
        "description": "plain-English explanation of what this measures",
        "type": "simple",
        "type_params": { "measure": "<measure name>" },
        "format": "..."
      },

      // --- RATIO (numerator / denominator, both are metrics) ---
      {
        "name": "...",
        "label": "...",
        "description": "...",
        "type": "ratio",
        "type_params": {
          "numerator": { "metric": "<metric name>", "filter": "<optional filter id>" },
          "denominator": { "metric": "<metric name>" }
        },
        "format": "..."
      },

      // --- DERIVED (expr over other metrics + optional parameters) ---
      {
        "name": "...",
        "label": "...",
        "description": "...",
        "type": "derived",
        "type_params": {
          "expr": "<neutral pseudo-formula — NO DAX, NO SAS, plain math>",
          "metrics": ["<metric name>", ...],
          "parameters": ["<parameter name>", ...]
        },
        "format": "..."
      }
    ],

    "parameters": [            // what-if / interactive prompts
      {
        "name": "<snake_case>",
        "label": "...",
        "description": "...",
        "data_type": "decimal | integer | string | date",
        "default": <value>,
        "range": { "min": <value>, "max": <value> },
        "affects_metrics": ["<metric name>", ...]
      }
    ]
  },

  "filters": [                 // reusable named filter definitions
    {
      "id": "<snake_case>",
      "label": "...",
      "type": "static | rank | parameter_driven",
      "scope": "report | page | visual",
      "dimension": "<dimension name>",
      // for static:
      "operator": "in | not_in | gt | lt | between | is_null | is_not_null",
      "values": [...],
      "include_nulls": true | false,
      // for rank:
      "rank_config": {
        "rank_by": "<metric name>",
        "group_by": "<dimension name>",
        "n": <integer>,
        "subset": "top | bottom",
        "include_ties": true | false
      }
    }
  ],

  "pages": [
    {
      "id": "<snake_case>",
      "display_name": "...",
      "description": "what business questions this page answers",
      "visuals": [
        {
          "id": "<snake_case>",
          "display_name": "...",
          "description": "what this visual shows and why",
          "visual_type": "<see mapping below>",
          "metrics": ["<metric name>", ...],
          "dimensions": ["<dimension name>", ...],
          "applied_filters": ["<filter id>", ...],
          "sort": [{ "field": "<metric or dimension name>", "direction": "asc | desc" }],
          "spec": { <Vega-Lite spec — see rules below> }
        }
      ]
    }
  ]
}

──────────────────────────────────────────────────────────────────
VISUAL TYPE MAPPING  (SAS graphType → canonical visual_type)
──────────────────────────────────────────────────────────────────
dualAxisBarLine → combo_chart
keyValue        → kpi_card
geo             → map
treeMap         → treemap
barChart        → bar_chart
lineChart       → line_chart
pieChart        → pie_chart
scatterPlot     → scatter_plot
Table (type)    → table
Crosstab (type) → crosstab
Prompt (type)   → filter_control
Text (type)     → text

──────────────────────────────────────────────────────────────────
VEGA-LITE SPEC RULES
──────────────────────────────────────────────────────────────────
1. Always use named data references — NEVER inline data values:
   "data": { "name": "<metric_or_dataset_id>" }

2. Field names in encoding must match canonical metric/dimension names
   (snake_case, from semantic_model), NOT SAS internal names (bi36 etc.)

3. Mark types per visual_type:
   bar_chart    → "mark": "bar"
   line_chart   → "mark": "line"
   combo_chart  → "layer": [ bar layer, line layer ] with "resolve": {"scale": {"y": "independent"}}
   kpi_card     → "mark": {"type": "text", "fontSize": 28, "fontWeight": "bold"}
   scatter_plot → "mark": "point"
   treemap      → "mark": "rect"  (note: full Vega needed for real treemap)
   map          → "mark": "geoshape"  with "projection": {"type": "mercator"}
   table        → omit spec (tabular, no Vega-Lite representation)
   crosstab     → omit spec
   filter_control → omit spec
   text         → omit spec

4. Encoding channels: x, y, color, size, text, latitude, longitude, shape
   Each has: "field", "type" (nominal/ordinal/quantitative/temporal), optional "title", "aggregate"

5. For dual-axis (combo_chart), use resolve for independent y axes:
   "resolve": { "scale": { "y": "independent" } }

6. Include $schema:
   "$schema": "https://vega.github.io/schema/vega-lite/v6.json"

──────────────────────────────────────────────────────────────────
PSEUDO-FORMULA RULES (for derived metric expr)
──────────────────────────────────────────────────────────────────
Use plain mathematical notation referencing canonical metric names:
  sum(sales)              → aggregate sum of the 'sales' measure
  average(marketing_budget)
  (sum(sales) - sum(cost)) / sum(cost)
  IF condition THEN expr1 ELSE expr2
  metric_name * (1 + parameter_name)

NO SAS syntax (div, aggregate, cond, in, ${...}, #{...})
NO DAX syntax (CALCULATE, DIVIDE, SUMX, etc.)
"""

# ── SAS expression quick-reference (helps Claude translate) ───────────────────

SAS_EXPR_GUIDE = """
SAS VA Expression → Neutral Formula reference:
  div(a, b)                     →  a / b
  times(a, b)                   →  a * b
  plus(a, b)                    →  a + b
  minus(a, b)                   →  a - b
  aggregate(sum, group, ${biXX,raw})    →  sum(<resolved_column>)
  aggregate(average, group, ${biXX,raw}) →  average(<resolved_column>)
  cond(condition, true_val, false_val)  →  IF condition THEN true_val ELSE false_val
  in(${biXX,binned}, 'A','B')          →  <dimension> IN ('A','B')
  ismissing(${biXX,binned})            →  <dimension> IS NULL
  #{prXX}                              →  <parameter_name>  (look up in parameters)
  ${biXX,raw}                          →  look up biXX in the xref table below
"""


# ── Agent 2 system prompt ─────────────────────────────────────────────────────

SYSTEM_PROMPT = f"""
You are Agent 2 in a SAS Visual Analytics → Power BI migration pipeline: the Canonical Model Builder.

Your job is to read Agent 1's structured output and produce a single, self-contained
canonical_model.json that is:
  - Vendor-neutral (no SAS syntax, no DAX, no Power BI specifics)
  - Complete (covers all data sources, fields, metrics, parameters, filters, pages, visuals)
  - Precise (all internal SAS names like bi36 resolved to human-readable canonical names)
  - Machine-readable (valid JSON, consistent naming, correct schema)

{CANONICAL_SCHEMA}

{SAS_EXPR_GUIDE}

Output rules:
- Respond with ONLY the canonical_model.json content inside a single ```json fence.
- No explanation text outside the fence.
- All names must be snake_case.
- Every metric referenced in a visual must exist in semantic_model.metrics.
- Every filter referenced in a visual must exist in the top-level filters array.
- Vega-Lite specs must use named data references only (no inline values).
- For visuals with no Vega-Lite representation (table, crosstab, filter_control, text),
  omit the "spec" key entirely.
"""


# ── Load Agent 1 artifacts ────────────────────────────────────────────────────

def load_artifacts(input_dir: Path) -> dict:
    """Load all Agent 1 JSON artifacts and documentation."""
    files = {
        "metadata":     "metadata.json",
        "sections":     "sections.json",
        "elements":     "elements.json",
        "data_sources": "data_sources.json",
        "calculations": "calculations.json",
        "visuals":      "visuals.json",
        "filters":      "filters.json",
        "documentation": "documentation.md",
    }
    artifacts = {}
    for key, filename in files.items():
        path = input_dir / filename
        if not path.exists():
            print(f"  [Warning] {filename} not found — skipping")
            continue
        text = path.read_text(encoding="utf-8")
        if filename.endswith(".json"):
            artifacts[key] = json.loads(text)
        else:
            artifacts[key] = text
    return artifacts


def build_xref_lookup(data_sources: dict) -> dict:
    """Build bi-name → CAS column name lookup from data_sources.json."""
    lookup = {}
    for src in data_sources.get("data_sources", []):
        for col in src.get("columns", []):
            if col.get("xref"):
                lookup[col["name"]] = {
                    "xref": col["xref"],
                    "label": col.get("label", ""),
                    "source": src["name"],
                }
        for calc in src.get("calculations", []):
            lookup[calc["name"]] = {
                "label": calc.get("label", ""),
                "type": calc.get("type", ""),
                "expression": calc.get("expression", ""),
                "source": src["name"],
            }
    return lookup


def build_user_message(report_id: str, artifacts: dict, xref: dict) -> str:
    """Construct the full context message for Agent 2."""
    parts = [
        f"## Task\nProduce canonical_model.json for report: {report_id}\n",
        "\n## Field Name Resolver (bi-name → CAS column / calculation)\n```json\n"
        + json.dumps(xref, indent=2)
        + "\n```",
    ]

    for key in ["metadata", "sections", "data_sources", "calculations", "visuals", "filters"]:
        if key in artifacts:
            parts.append(
                f"\n## {key.replace('_', ' ').title()} (from Agent 1)\n```json\n"
                + json.dumps(artifacts[key], indent=2)
                + "\n```"
            )

    if "documentation" in artifacts:
        # Include a trimmed version of the documentation (first 6000 chars)
        doc = artifacts["documentation"][:6000]
        parts.append(f"\n## Agent 1 Documentation (excerpt)\n{doc}")

    return "\n".join(parts)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Agent 2 — Canonical Model Builder")
    parser.add_argument("--report-id", default="cbf97b0a-457d-4b4f-8913-547e0cdf390c")
    parser.add_argument("--input-dir", default="")
    args = parser.parse_args()

    input_dir = Path(args.input_dir or f"docs/{args.report_id}")
    if not input_dir.exists():
        sys.exit(f"ERROR: Input directory not found: {input_dir}\nRun agent1_documenter.py first.")

    print(f"\n{'='*60}")
    print(f"  Agent 2 — Canonical Model Builder")
    print(f"  Input  : {input_dir}")
    print(f"  Output : {input_dir / OUTPUT_FILE}")
    print(f"{'='*60}\n")

    # Load artifacts
    print("[1/4] Loading Agent 1 artifacts...")
    artifacts = load_artifacts(input_dir)
    print(f"      Loaded: {list(artifacts.keys())}")

    # Build xref lookup
    print("[2/4] Building field name resolver...")
    xref = build_xref_lookup(artifacts.get("data_sources", {}))
    print(f"      Resolved {len(xref)} field references")

    # Build prompt
    print("[3/4] Calling Claude to build canonical model...")
    user_message = build_user_message(args.report_id, artifacts, xref)

    client = anthropic.Anthropic()
    messages = [{"role": "user", "content": user_message}]
    all_text = ""

    # Agentic loop (handles max_tokens continuation)
    for iteration in range(10):
        response = client.messages.create(
            model=MODEL,
            max_tokens=MAX_TOKENS,
            system=SYSTEM_PROMPT,
            messages=messages,
        )
        print(f"      Iteration {iteration+1}: stop={response.stop_reason} "
              f"in={response.usage.input_tokens} out={response.usage.output_tokens}")

        for block in response.content:
            if hasattr(block, "text"):
                all_text += block.text

        if response.stop_reason == "end_turn":
            break

        if response.stop_reason == "max_tokens":
            messages.append({"role": "assistant", "content": response.content})
            messages.append({"role": "user", "content": "Continue exactly where you left off."})
            continue

        break

    # Extract JSON from response
    print("[4/4] Extracting and saving canonical_model.json...")
    canonical_json = None

    if "```json" in all_text:
        start = all_text.index("```json") + 7
        end = all_text.rindex("```")
        canonical_json = all_text[start:end].strip()
    elif "```" in all_text:
        start = all_text.index("```") + 3
        nl = all_text.index("\n", start)
        end = all_text.rindex("```")
        canonical_json = all_text[nl:end].strip()
    else:
        canonical_json = all_text.strip()

    # Validate JSON
    try:
        parsed = json.loads(canonical_json)
    except json.JSONDecodeError as e:
        print(f"\n[ERROR] Response is not valid JSON: {e}")
        debug_path = input_dir / "canonical_model_raw.txt"
        debug_path.write_text(all_text, encoding="utf-8")
        print(f"  Raw response saved to: {debug_path}")
        sys.exit(1)

    # Save
    out_path = input_dir / OUTPUT_FILE
    out_path.write_text(json.dumps(parsed, indent=2, ensure_ascii=False), encoding="utf-8")

    # Summary
    sm = parsed.get("semantic_model", {})
    print(f"\n  ✓ canonical_model.json saved ({out_path.stat().st_size // 1024}KB)")
    print(f"    sources    : {len(sm.get('sources', []))}")
    print(f"    dimensions : {len(sm.get('dimensions', []))}")
    print(f"    measures   : {len(sm.get('measures', []))}")
    print(f"    metrics    : {len(sm.get('metrics', []))}")
    print(f"    parameters : {len(sm.get('parameters', []))}")
    print(f"    filters    : {len(parsed.get('filters', []))}")
    print(f"    pages      : {len(parsed.get('pages', []))}")
    total_visuals = sum(len(p.get('visuals', [])) for p in parsed.get('pages', []))
    print(f"    visuals    : {total_visuals}")


if __name__ == "__main__":
    main()
