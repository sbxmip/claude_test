"""
End-to-end test: Claude generates a synthetic dataset → Parquet → Fabric Delta table.

Flow
----
1. Ask Claude (claude-haiku-4-5) to invent a small dataset (topic + rows as JSON).
2. Parse the JSON, convert to a PyArrow table, write to a temp Parquet file.
3. Call write_delta_table to push it to lakehouse_testcases on Fabric.
4. Call list_tables to confirm the table is now visible via SQL endpoint.
5. Clean up (best-effort: the table stays in Fabric for manual inspection).

Run with:
    pytest fabric_mcp/test_claude_generated_dataset.py -v -s
"""

import importlib.util
import json
import os
import re
import tempfile
from pathlib import Path

import anthropic
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

# ── Load server module ─────────────────────────────────────────────────────────

_spec = importlib.util.spec_from_file_location(
    "fabric_server",
    Path(__file__).parent / "server.py",
)
_m = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_m)

# ── Constants ─────────────────────────────────────────────────────────────────

LAKEHOUSE_ID   = "7f793f2a-34e0-45eb-936c-7f73a3e66910"   # lakehouse_testcases
TARGET_SCHEMA  = "dbo"
TARGET_TABLE   = "claude_generated_test"
CLAUDE_MODEL   = "claude-haiku-4-5-20251001"

PROMPT = """
Generate a small dataset of 10 rows that could realistically represent
synthetic sales transactions for a retail store.

Respond with ONLY a JSON object in this exact format:
{
  "description": "<one sentence describing the dataset>",
  "columns": ["col1", "col2", ...],
  "rows": [
    [val1, val2, ...],
    ...
  ]
}

Rules:
- 5–7 columns: mix of integers, floats, and strings.
- Column names must be valid SQL identifiers (lowercase, underscores only).
- String values must be realistic but fictional.
- No nulls.
"""

# ── Fixtures ───────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def workspace_id():
    wid = os.environ.get("FABRIC_WORKSPACE_ID")
    if not wid:
        pytest.skip("FABRIC_WORKSPACE_ID not set")
    return wid


@pytest.fixture(scope="module", autouse=True)
def auth():
    result = json.loads(_m.authenticate())
    assert result.get("fabric") == "ok"
    assert result.get("storage") == "ok"


# ── Helpers ────────────────────────────────────────────────────────────────────

def _ask_claude_for_dataset() -> dict:
    """Call Claude and return the parsed dataset dict."""
    client = anthropic.Anthropic()
    message = client.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=1024,
        messages=[{"role": "user", "content": PROMPT}],
    )
    raw = message.content[0].text.strip()

    # Extract JSON even if Claude wraps it in a code fence
    match = re.search(r"\{.*\}", raw, re.DOTALL)
    if not match:
        raise ValueError(f"Claude response did not contain JSON:\n{raw}")
    return json.loads(match.group())


def _dataset_to_parquet(dataset: dict, path: str) -> pa.Table:
    """Convert the Claude-generated dataset dict to a Parquet file."""
    columns = dataset["columns"]
    rows    = dataset["rows"]

    # Transpose rows → column arrays
    col_arrays = {col: [row[i] for row in rows] for i, col in enumerate(columns)}

    table = pa.table(col_arrays)
    pq.write_table(table, path)
    return table


# ── Test ───────────────────────────────────────────────────────────────────────

def test_claude_generated_dataset_upload(workspace_id):
    """
    Ask Claude to invent a dataset, write it as Parquet, upload to Fabric,
    and verify the table appears in list_tables.
    """

    # ── Step 1: Claude generates the dataset ──────────────────────────────────
    print("\n[1/4] Asking Claude to generate a dataset...")
    dataset = _ask_claude_for_dataset()

    print(f"      Description : {dataset['description']}")
    print(f"      Columns     : {dataset['columns']}")
    print(f"      Rows        : {len(dataset['rows'])}")

    assert len(dataset["columns"]) >= 5, "Expected at least 5 columns"
    assert len(dataset["rows"]) >= 5,    "Expected at least 5 rows"

    # ── Step 2: Write to Parquet ──────────────────────────────────────────────
    print("[2/4] Converting to Parquet...")
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        tmp_path = f.name

    try:
        pa_table = _dataset_to_parquet(dataset, tmp_path)
        size_kb  = Path(tmp_path).stat().st_size / 1024
        print(f"      {pa_table.num_rows} rows × {pa_table.num_columns} cols  "
              f"({size_kb:.1f} KB)  →  {tmp_path}")

        # ── Step 3: Upload to Fabric ──────────────────────────────────────────
        print(f"[3/4] Uploading to Fabric lakehouse_testcases "
              f"as {TARGET_SCHEMA}.{TARGET_TABLE}...")

        result = json.loads(_m.write_delta_table(
            workspace_id   = workspace_id,
            lakehouse_id   = LAKEHOUSE_ID,
            schema         = TARGET_SCHEMA,
            table_name     = TARGET_TABLE,
            local_parquet_path = tmp_path,
            mode           = "overwrite",
        ))

        print(f"      Result: {json.dumps(result)}")
        assert result.get("status") == "ok",  f"write_delta_table failed: {result}"
        assert result["rows"] == pa_table.num_rows
        assert result["delta_version"] is not None

        # ── Step 4: Verify via list_tables ────────────────────────────────────
        # Fabric's SQL analytics endpoint auto-discovers Delta tables written
        # externally (via delta-rs) on a background cycle that can take up to
        # ~10 minutes. We poll every 30 s for up to 15 minutes.
        print("[4/4] Verifying table appears in list_tables (up to 15 min)...")
        import time as _time
        table_names = []
        for attempt in range(30):          # 30 × 30 s = 15 min max
            tables = json.loads(_m.list_tables(workspace_id, LAKEHOUSE_ID))
            table_names = [t["name"] for t in tables]
            if TARGET_TABLE in table_names:
                elapsed = attempt * 30
                print(f"      Visible after ~{elapsed} s")
                break
            print(f"      [{attempt+1}/30] not visible yet — retrying in 30 s...")
            _time.sleep(30)

        dbo_tables = [t["name"] for t in tables if t["schema"] == "dbo"]
        print(f"      dbo tables: {dbo_tables}")
        assert TARGET_TABLE in table_names, (
            f"Table '{TARGET_TABLE}' not found after 15 min. dbo tables: {dbo_tables}"
        )

        print(f"\n  All steps passed. "
              f"Table '{TARGET_SCHEMA}.{TARGET_TABLE}' is live on Fabric.")

    finally:
        Path(tmp_path).unlink(missing_ok=True)
