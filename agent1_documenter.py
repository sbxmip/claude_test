#!/usr/bin/env python3
"""
Agent 1 — SAS VA Documentation Agent
=====================================
Part of the SAS Visual Analytics → Power BI migration pipeline.

Connects to the local SAS VA MCP server, authenticates to Viya, gathers every
report artifact (metadata, sections, data sources, calculations, visuals,
filters, screenshots), visually inspects each page, and writes:

    docs/<report_id>/
        metadata.json
        sections.json
        elements.json
        data_sources.json
        calculations.json
        visuals.json
        filters.json
        screenshots/          ← one PNG per visible section
        documentation.md      ← the main human-readable output (fed to Agent 2)

Usage:
    python agent1_documenter.py [--report-id UUID] [--output-dir PATH]

Environment:
    VIYA_BASE_URL   (default: https://harvai.westeurope.cloudapp.azure.com)
    VIYA_USERNAME
    VIYA_PASSWORD
    ANTHROPIC_API_KEY
"""

import argparse
import asyncio
import base64
import json
import os
import sys
from pathlib import Path

import anthropic
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

# ── Config ────────────────────────────────────────────────────────────────────

BASE_URL = os.environ.get("VIYA_BASE_URL", "https://harvai.westeurope.cloudapp.azure.com")
USERNAME = os.environ.get("VIYA_USERNAME", "")
PASSWORD = os.environ.get("VIYA_PASSWORD", "")
MCP_SERVER_PATH = str(Path(__file__).parent / "sas_va_mcp" / "server.py")

MODEL = "claude-opus-4-6"
MAX_TOKENS = 16000
MAX_ITERATIONS = 60          # safety cap on agentic loop turns

# ── System prompt ─────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """\
You are Agent 1 in a SAS Visual Analytics → Power BI migration pipeline: the Documentation Agent.

Your output (documentation.md) will be the primary input for:
- Agent 2: builds a vendor-neutral canonical data model
- Agent 3: designs the Power BI / Fabric semantic model
- Agent 4: implements the actual Power BI reports

─── TASK ───────────────────────────────────────────────────────────────────────
You will be given a REPORT_ID, BASE_URL, USERNAME, PASSWORD, and OUTPUT_DIR.
Follow these steps IN ORDER:

1. Call `authenticate` with the provided credentials.
2. Call `document_report(report_id, output_dir)` — this saves all JSON artifacts
   to disk and returns a summary (sections, data sources, calculation counts, etc.)
3. Examine the full data by calling:
   - `get_report_sections` — confirm visible vs hidden pages
   - `parse_data_sources` — CAS tables, columns, calculations
   - `parse_calculations` — all measures with SAS expressions
   - `parse_visual_elements` — chart types, data bindings
   - `parse_filters_and_prompts` — interactive parameters, filters, ranks
   ⚠️  Do NOT call `get_report_content_xml` — it returns raw XML that is too large.
       All structured data you need is provided by the parse_* tools above.
4. For EACH visible section call `get_section_screenshot(report_id, section_index)`.
   You will SEE the actual chart. Describe what you see accurately.
5. Write documentation.md (described below) to OUTPUT_DIR.

─── OUTPUT FORMAT (documentation.md) ───────────────────────────────────────────
Produce a markdown file with these sections:

# Report: <name>
**Report ID:** <uuid>  **Last modified:** <date>  **Created by:** <user>

## 1. Executive Summary
What business domain does this report cover? Who would use it and why?
What are the 2-3 key questions it answers?

## 2. Report Structure
List all visible sections/pages. For each: name, purpose, key visuals.

## 3. Data Model
For each data source:
- Source name, CAS path (library.table)
- List of key columns used across visuals (name, apparent data type, role: dim/measure)
- Hierarchies defined

## 4. Calculations & Business Logic
Table with columns: Measure Name | Type | SAS Expression | Plain-English Meaning
Include ALL AggregateCalculatedItem, CalculatedItem, GroupedItem.
For each SAS expression, translate it to plain English so a Power BI developer
unfamiliar with SAS can understand the intent.

## 5. Visual Inventory
For each section, list each visual:
| Visual | Type | Chart Type | Measures | Dimensions | Filters | Business Question |
Include observations from the actual screenshot (colours, trends, layout).

## 6. Interactivity & Navigation
- Parameters/prompts: name, default value, how it affects calculations
- Report-level and section-level filters
- Rank/Top-N rules
- Navigation actions between sections

## 7. Migration Notes for Power BI
- DAX equivalents for each SAS calculated measure
- Data preparation steps needed (binning, grouping, custom regions)
- Potential challenges (multidimensional queries, Esri maps, etc.)

─── RULES ───────────────────────────────────────────────────────────────────────
- Be precise. Do not invent data — only document what the tools return.
- For SAS expressions use the actual expression text from parse_calculations.
- DAX equivalents should be syntactically correct DAX; mark uncertain ones with ⚠️.
- Save documentation.md by writing it as a file in OUTPUT_DIR using Python's
  open() inside a tool call... but you don't have a file-write tool. Instead,
  emit the COMPLETE documentation.md content in your final message, clearly
  delimited with ```markdown fences, and the orchestrator will save it.
"""

# ── Tool result processor ─────────────────────────────────────────────────────

def _build_tool_result(tool_use_id: str, mcp_result) -> dict:
    """
    Convert an MCP call_tool result into an Anthropic tool_result block.
    If the result JSON contains a `base64_png` key, also attach the image so
    Claude can visually inspect the screenshot.
    """
    # MCP returns a list of content items; typically one TextContent
    raw_text = ""
    for item in mcp_result.content:
        if hasattr(item, "text"):
            raw_text += item.text

    # Try to detect a screenshot result and extract the image
    content_blocks = []
    try:
        parsed = json.loads(raw_text)
        if "base64_png" in parsed:
            # Summarise metadata as text (drop the huge base64 field)
            meta = {k: v for k, v in parsed.items() if k != "base64_png"}
            content_blocks.append({"type": "text", "text": json.dumps(meta)})
            content_blocks.append({
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": "image/png",
                    "data": parsed["base64_png"],
                },
            })
        else:
            content_blocks.append({"type": "text", "text": raw_text})
    except (json.JSONDecodeError, Exception):
        content_blocks.append({"type": "text", "text": raw_text})

    return {
        "type": "tool_result",
        "tool_use_id": tool_use_id,
        "content": content_blocks,
    }


# ── Agentic loop ──────────────────────────────────────────────────────────────

async def run_agent(report_id: str, output_dir: str) -> None:
    print(f"\n{'='*60}")
    print(f"  Agent 1 — SAS VA Documentation Agent")
    print(f"  Report : {report_id}")
    print(f"  Output : {output_dir}")
    print(f"{'='*60}\n")

    Path(output_dir).mkdir(parents=True, exist_ok=True)

    server_params = StdioServerParameters(
        command=sys.executable,
        args=[MCP_SERVER_PATH],
        env=dict(os.environ),
    )

    async with stdio_client(server_params) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()

            # Get tool list from MCP server
            tools_response = await session.list_tools()
            tools = [
                {
                    "name": t.name,
                    "description": t.description or "",
                    "input_schema": t.inputSchema,
                }
                for t in tools_response.tools
            ]
            print(f"MCP tools available: {[t['name'] for t in tools]}\n")

            # Kick off the agent
            initial_message = (
                f"Please document this SAS VA report and produce documentation.md.\n\n"
                f"REPORT_ID : {report_id}\n"
                f"BASE_URL  : {BASE_URL}\n"
                f"USERNAME  : {USERNAME}\n"
                f"PASSWORD  : {PASSWORD}\n"
                f"OUTPUT_DIR: {output_dir}\n\n"
                f"Follow the steps in your system prompt. "
                f"Call get_section_screenshot for each visible section so you can see "
                f"the actual charts before writing the Visual Inventory section."
            )

            messages: list[dict] = [{"role": "user", "content": initial_message}]
            client = anthropic.Anthropic()

            for iteration in range(MAX_ITERATIONS):
                print(f"[Iteration {iteration + 1}] Calling {MODEL}...")

                response = client.messages.create(
                    model=MODEL,
                    max_tokens=MAX_TOKENS,
                    system=SYSTEM_PROMPT,
                    tools=tools,
                    messages=messages,
                )

                print(f"  stop_reason: {response.stop_reason}  |  "
                      f"input_tokens: {response.usage.input_tokens}  "
                      f"output_tokens: {response.usage.output_tokens}")

                # Append assistant turn
                messages.append({"role": "assistant", "content": response.content})

                # ── Done ──────────────────────────────────────────────────────
                if response.stop_reason == "end_turn":
                    print("\n[Agent finished]\n")

                    # Collect all text across every assistant turn
                    all_text = ""
                    for msg in messages:
                        if msg["role"] != "assistant":
                            continue
                        content = msg["content"]
                        if isinstance(content, list):
                            for block in content:
                                if hasattr(block, "text"):
                                    all_text += block.text
                        elif isinstance(content, str):
                            all_text += content

                    print(all_text[-3000:])  # preview last 3000 chars

                    # Extract markdown content (try ```markdown, then ```, then raw)
                    doc_content = None
                    if "```markdown" in all_text:
                        start = all_text.index("```markdown") + len("```markdown")
                        end = all_text.rindex("```")
                        doc_content = all_text[start:end].strip()
                    elif "```" in all_text:
                        start = all_text.index("```") + 3
                        nl = all_text.index("\n", start)
                        end = all_text.rindex("```")
                        doc_content = all_text[nl:end].strip()
                    else:
                        # No fences — use everything after the last tool call section
                        doc_content = all_text.strip()

                    if doc_content:
                        doc_path = Path(output_dir) / "documentation.md"
                        doc_path.write_text(doc_content, encoding="utf-8")
                        print(f"\n[Saved] {doc_path}  ({len(doc_content)} chars)")
                    else:
                        print("\n[Warning] Could not extract documentation — check output above.")
                    break

                # ── Tool use ──────────────────────────────────────────────────
                if response.stop_reason == "tool_use":
                    tool_results = []
                    for block in response.content:
                        if block.type != "tool_use":
                            continue

                        print(f"  → {block.name}({json.dumps(block.input)[:120]})")
                        try:
                            mcp_result = await session.call_tool(block.name, block.input)
                            tool_result = _build_tool_result(block.id, mcp_result)
                        except Exception as exc:
                            print(f"     ERROR: {exc}")
                            tool_result = {
                                "type": "tool_result",
                                "tool_use_id": block.id,
                                "content": json.dumps({"error": str(exc)}),
                                "is_error": True,
                            }
                        tool_results.append(tool_result)

                    messages.append({"role": "user", "content": tool_results})
                    continue

                # Hit output token limit mid-generation — ask Claude to continue
                if response.stop_reason == "max_tokens":
                    print("  [max_tokens hit — continuing...]")
                    messages.append({
                        "role": "user",
                        "content": "Please continue exactly where you left off.",
                    })
                    continue

                print(f"[Warning] Unexpected stop_reason: {response.stop_reason}")
                break

            else:
                print(f"[Warning] Reached max iterations ({MAX_ITERATIONS}) without finishing.")


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Agent 1 — SAS VA Documentation Agent")
    parser.add_argument(
        "--report-id",
        default="cbf97b0a-457d-4b4f-8913-547e0cdf390c",
        help="SAS VA report UUID",
    )
    parser.add_argument(
        "--output-dir",
        default="",
        help="Output directory (default: docs/<report-id>)",
    )
    args = parser.parse_args()

    if not USERNAME or not PASSWORD:
        sys.exit("ERROR: Set VIYA_USERNAME and VIYA_PASSWORD environment variables.")

    output_dir = args.output_dir or f"docs/{args.report_id}"
    asyncio.run(run_agent(args.report_id, output_dir))


if __name__ == "__main__":
    main()
