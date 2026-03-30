"""
Integration tests for deploy_semantic_model, refresh_semantic_model,
deploy_report, list_semantic_models, list_reports.

Requires environment variables:
  FABRIC_TENANT_ID, FABRIC_CLIENT_ID, FABRIC_CLIENT_SECRET, FABRIC_WORKSPACE_ID

The tests build a minimal DirectLake Semantic Model against the
dbo.claude_generated_test Delta table in lakehouse_testcases, deploy it,
refresh (frame) it, deploy a thin Report on top, then clean up.

Run with:
  pytest fabric_mcp/test_pbi_deploy.py -v -s
"""

import importlib.util
import json
import os
import tempfile
import textwrap
from pathlib import Path

import pytest


# ── Load server module ─────────────────────────────────────────────────────────

_spec = importlib.util.spec_from_file_location(
    "fabric_server",
    Path(__file__).parent / "server.py",
)
_m = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_m)

# ── Constants ─────────────────────────────────────────────────────────────────

WORKSPACE_ID   = "1c20ddfa-15ef-4959-983e-3b4c71709dd6"
LAKEHOUSE_ID   = "7f793f2a-34e0-45eb-936c-7f73a3e66910"   # lakehouse_testcases
SQL_ENDPOINT   = ("wdq5gjizki2e7hmjiv5ebwjtry-7losahhpcvmutgb6hnghc4e52y"
                  ".datawarehouse.fabric.microsoft.com")
SQL_DATABASE   = "lakehouse_testcases"
SOURCE_TABLE   = "claude_generated_test"
SOURCE_SCHEMA  = "dbo"
ONELAKE_TABLES = (f"https://onelake.dfs.fabric.microsoft.com"
                  f"/{WORKSPACE_ID}/{LAKEHOUSE_ID}/Tables/")

SM_NAME        = "pytest_sm_import"
REPORT_NAME    = "pytest_report_import"

# ── Minimal TMDL content (Import mode via SQL Analytics Endpoint) ─────────────
#
# DirectLake framing requires a Fabric workspace connection that must be set up
# by a Fabric admin (cannot be created programmatically via service principal).
# We use Import mode instead: the SQL Analytics Endpoint is queried during
# refresh and the data is cached in the model — fully functional for reports.

_DEFINITION_PBISM = json.dumps({"version": "4.0", "settings": {}}, indent=2)

_DATABASE_TMDL = textwrap.dedent("""\
    database {SOURCE_TABLE}
        compatibilityLevel: 1567
""").format(SOURCE_TABLE=SOURCE_TABLE)

_MODEL_TMDL = textwrap.dedent("""\
    model Model
        defaultPowerBIDataSourceVersion: powerBI_V3
        culture: en-US
""")

_EXPRESSIONS_TMDL = textwrap.dedent("""\
    expression 'DatabaseQuery' =
            let
                Source = Sql.Database("{SQL_ENDPOINT}", "{SQL_DATABASE}")
            in
                Source
        lineageTag: 00000000-0000-0000-0000-000000000001

""").format(SQL_ENDPOINT=SQL_ENDPOINT, SQL_DATABASE=SQL_DATABASE)

_TABLE_TMDL = textwrap.dedent("""\
    table {SOURCE_TABLE}
        lineageTag: 00000000-0000-0000-0000-000000000002

        partition {SOURCE_TABLE} = m
            mode: import
            source =
                let
                    Source = Sql.Database("{SQL_ENDPOINT}", "{SQL_DATABASE}"),
                    Table  = Source{{[Schema="{SOURCE_SCHEMA}", Item="{SOURCE_TABLE}"]}}[Data]
                in
                    Table

        annotation PBI_ResultType = Table
""").format(
    SOURCE_TABLE=SOURCE_TABLE,
    SOURCE_SCHEMA=SOURCE_SCHEMA,
    SQL_ENDPOINT=SQL_ENDPOINT,
    SQL_DATABASE=SQL_DATABASE,
)


# SQL data-type → TMDL dataType mapping
_SQL_TO_TMDL = {
    "bigint": "int64", "int": "int64", "smallint": "int64", "tinyint": "int64",
    "float": "double", "real": "double", "decimal": "decimal", "numeric": "decimal",
    "money": "decimal", "smallmoney": "decimal",
    "varchar": "string", "nvarchar": "string", "char": "string", "nchar": "string",
    "text": "string", "ntext": "string",
    "date": "dateTime", "datetime": "dateTime", "datetime2": "dateTime",
    "datetimeoffset": "dateTimeOffset", "time": "dateTime",
    "bit": "boolean",
}


def _query_columns(sql_endpoint: str, sql_database: str,
                   schema: str, table: str) -> list[tuple[str, str]]:
    """Return [(column_name, tmdl_dataType), ...] from INFORMATION_SCHEMA."""
    conn = _m._sql_connect(sql_endpoint, sql_database)
    cur = conn.cursor()
    cur.execute("""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
    """, schema, table)
    return [
        (row[0], _SQL_TO_TMDL.get(row[1].lower(), "string"))
        for row in cur.fetchall()
    ]


def _profile_columns(sql_endpoint: str, sql_database: str,
                     schema: str, table: str,
                     columns: list[tuple[str, str]]) -> list[dict]:
    """
    Return per-column statistics used to classify dimensions vs measures.

    For each column: name, tmdl_dataType, total_rows, distinct_count,
    and (for numeric) min/max; (for string) up to 5 sample values.
    """
    conn = _m._sql_connect(sql_endpoint, sql_database)
    cur = conn.cursor()

    cur.execute(f"SELECT COUNT(*) FROM [{schema}].[{table}]")
    total_rows = cur.fetchone()[0]

    profiles = []
    for name, dtype in columns:
        prof: dict = {"name": name, "dtype": dtype, "total_rows": total_rows}

        cur.execute(f"SELECT COUNT(DISTINCT [{name}]) FROM [{schema}].[{table}]")
        prof["distinct_count"] = cur.fetchone()[0]

        if dtype in ("int64", "double", "decimal"):
            cur.execute(
                f"SELECT MIN([{name}]), MAX([{name}]) FROM [{schema}].[{table}]"
            )
            row = cur.fetchone()
            prof["min"], prof["max"] = row[0], row[1]
        else:
            cur.execute(
                f"SELECT DISTINCT TOP 5 [{name}] FROM [{schema}].[{table}]"
            )
            prof["samples"] = [r[0] for r in cur.fetchall()]

        profiles.append(prof)

    return profiles


def _classify_columns_with_claude(profiles: list[dict]) -> dict[str, str]:
    """
    Ask Claude to classify each column as 'measure' or 'dimension'.

    Returns {column_name: 'sum'|'none'} (TMDL summarizeBy values).
    """
    import anthropic, re

    lines = []
    for p in profiles:
        row = (f"- {p['name']} ({p['dtype']}): "
               f"{p['distinct_count']} distinct / {p['total_rows']} rows")
        if "samples" in p:
            row += f", samples: {p['samples']}"
        if "min" in p:
            row += f", range: {p['min']} – {p['max']}"
        lines.append(row)

    prompt = f"""\
You are classifying columns in a database table for a Power BI semantic model.

For each column decide:
- "measure"    → numeric column that makes business sense to SUM (e.g. amount, quantity, price)
- "dimension"  → column used for grouping/filtering (text, dates, IDs, codes, flags,
                  or numeric columns with very high cardinality relative to rows
                  that are clearly keys or codes)

Column profiles (name, type, distinct count / total rows, samples or range):
{chr(10).join(lines)}

Reply with ONLY a JSON object mapping each column name to "measure" or "dimension".
Example: {{"col1": "dimension", "col2": "measure"}}
"""

    client = anthropic.Anthropic()
    msg = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=512,
        messages=[{"role": "user", "content": prompt}],
    )
    raw = msg.content[0].text.strip()
    match = re.search(r"\{.*\}", raw, re.DOTALL)
    classification = json.loads(match.group())

    return {
        name: ("sum" if classification.get(name, "dimension") == "measure" else "none")
        for name, _ in [(p["name"], p["dtype"]) for p in profiles]
    }


def _columns_tmdl(columns: list[tuple[str, str]],
                  summarize_by: dict[str, str] | None = None) -> str:
    """
    Render TMDL column + explicit DAX measure definitions.

    Columns classified as measures (summarizeBy: sum) get summarizeBy: none on
    the column itself plus an explicit DAX measure (SUM) so Power BI visuals can
    reference them unambiguously as Measure objects.

    Returns the TMDL block AND populates a module-level list of measure names.
    """
    import uuid
    _MEASURE_TYPES = {"int64", "double", "decimal"}
    col_lines = []
    measure_lines = []
    for name, dtype in columns:
        tag = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"col-{name}"))
        if summarize_by:
            is_measure = summarize_by.get(name, "none") == "sum"
        else:
            is_measure = dtype in _MEASURE_TYPES
        # All columns: summarizeBy none (avoids implicit measure confusion)
        col_lines.append(
            f"    column {name}\n"
            f"        dataType: {dtype}\n"
            f"        lineageTag: {tag}\n"
            f"        sourceColumn: {name}\n"
            f"        summarizeBy: none\n"
        )
        if is_measure:
            msr_name = f"Sum_{name}"   # underscore avoids spaces in queryRef
            mtag = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"msr-{name}"))
            measure_lines.append(
                f"    measure {msr_name} = SUM('{SOURCE_TABLE}'[{name}])\n"
                f"        lineageTag: {mtag}\n"
                f"        formatString: #,0\n"
            )
    return "\n".join(col_lines) + ("\n" + "\n".join(measure_lines) if measure_lines else "")


def _write_semantic_model(base_dir: Path) -> Path:
    """Write a minimal .SemanticModel folder and return its path."""
    sm_dir = base_dir / f"{SM_NAME}.SemanticModel"
    sm_dir.mkdir(parents=True)

    # Root manifest
    (sm_dir / "definition.pbism").write_text(_DEFINITION_PBISM, encoding="utf-8")

    # definition/ subfolder
    def_dir = sm_dir / "definition"
    def_dir.mkdir()
    (def_dir / "database.tmdl").write_text(_DATABASE_TMDL, encoding="utf-8")
    (def_dir / "model.tmdl").write_text(_MODEL_TMDL, encoding="utf-8")
    (def_dir / "expressions.tmdl").write_text(_EXPRESSIONS_TMDL, encoding="utf-8")

    # Tables subfolder — discover columns, let Claude classify them, emit measures
    tables_dir = def_dir / "tables"
    tables_dir.mkdir()
    columns = _query_columns(SQL_ENDPOINT, SQL_DATABASE, SOURCE_SCHEMA, SOURCE_TABLE)
    profiles = _profile_columns(SQL_ENDPOINT, SQL_DATABASE, SOURCE_SCHEMA, SOURCE_TABLE, columns)
    summarize_by = _classify_columns_with_claude(profiles)
    print(f"\n  Column classification: {summarize_by}")
    col_block = _columns_tmdl(columns, summarize_by)
    table_tmdl = _TABLE_TMDL.replace(
        "    annotation PBI_ResultType = Table",
        f"{col_block}\n    annotation PBI_ResultType = Table",
    )
    (tables_dir / f"{SOURCE_TABLE}.tmdl").write_text(table_tmdl, encoding="utf-8")

    # Return metadata alongside path so the report builder can pick measure names
    measure_names = [f"Sum_{n}" for n, v in summarize_by.items() if v == "sum"]
    dim_names     = [n for n, v in summarize_by.items() if v == "none"
                     and next(d for c, d in columns if c == n) == "string"]
    return sm_dir, measure_names, dim_names


# PBIR-Legacy report: definition.pbir + report.json (single-file legacy layout).
# This is the format Fabric natively creates when you build a report via the UI.
# definition.pbir holds the semantic model connection; report.json holds the layout.

_DEFINITION_PBIR = json.dumps({
    "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definitionProperties/2.0.0/schema.json",
    "version": "4.0",
    "datasetReference": {
        "byPath": {"path": "../placeholder.SemanticModel"}
    }
}, indent=2)


def _build_report_json(dim_name: str, measure_col: str) -> str:
    """
    Build the PBIR-Legacy report.json with one columnChart visual.

    Uses prototypeQuery with Aggregation.Function=0 (Sum) directly on the
    source column — no named DAX measure required.
    """
    import uuid
    visual_name = uuid.uuid4().hex[:20]

    visual_config = {
        "name": visual_name,
        "layouts": [{
            "id": 0,
            "position": {
                "x": 10, "y": 0, "z": 0,
                "width": 1000, "height": 600,
                "tabOrder": 0,
            }
        }],
        "singleVisual": {
            "visualType": "columnChart",
            "projections": {
                "Category": [{"queryRef": f"{SOURCE_TABLE}.{dim_name}", "active": True}],
                "Y":        [{"queryRef": f"Sum({SOURCE_TABLE}.{measure_col})"}],
            },
            "prototypeQuery": {
                "Version": 2,
                "From": [{"Name": "c", "Entity": SOURCE_TABLE, "Type": 0}],
                "Select": [
                    {
                        "Column": {
                            "Expression": {"SourceRef": {"Source": "c"}},
                            "Property": dim_name,
                        },
                        "Name": f"{SOURCE_TABLE}.{dim_name}",
                        "NativeReferenceName": dim_name,
                    },
                    {
                        "Aggregation": {
                            "Expression": {
                                "Column": {
                                    "Expression": {"SourceRef": {"Source": "c"}},
                                    "Property": measure_col,
                                }
                            },
                            "Function": 0,
                        },
                        "Name": f"Sum({SOURCE_TABLE}.{measure_col})",
                        "NativeReferenceName": f"Sum of {measure_col}",
                    },
                ],
                "OrderBy": [{
                    "Direction": 2,
                    "Expression": {
                        "Aggregation": {
                            "Expression": {
                                "Column": {
                                    "Expression": {"SourceRef": {"Source": "c"}},
                                    "Property": measure_col,
                                }
                            },
                            "Function": 0,
                        }
                    }
                }],
            },
            "drillFilterOtherVisuals": True,
            "hasDefaultSort": True,
        },
    }

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
            "useNewFilterPaneExperience": True,
            "allowChangeFilterTypes": True,
            "useStylableVisualContainerHeader": True,
            "queryLimitOption": 6,
            "useEnhancedTooltips": True,
            "exportDataMode": 1,
            "useDefaultAggregateDisplayName": True,
        },
    }

    report = {
        "config": json.dumps(report_config),
        "layoutOptimization": 0,
        "resourcePackages": [],
        "sections": [{
            "config": "{}",
            "displayName": "Page 1",
            "displayOption": 1,
            "filters": "[]",
            "height": 720.0,
            "name": "ReportSection",
            "visualContainers": [{
                "config": json.dumps(visual_config),
                "filters": "[]",
                "height": 600.0,
                "width": 1000.0,
                "x": 10.0,
                "y": 0.0,
                "z": 0.0,
            }],
            "width": 1280.0,
        }],
    }
    return json.dumps(report, indent=2)


def _write_report(base_dir: Path,
                  measure_names: list[str] | None = None,
                  dim_names: list[str] | None = None) -> Path:
    """Write a minimal PBIR-Legacy .Report folder and return its path."""
    rpt_dir = base_dir / f"{REPORT_NAME}.Report"
    rpt_dir.mkdir(parents=True)

    (rpt_dir / "definition.pbir").write_text(_DEFINITION_PBIR, encoding="utf-8")

    # Derive raw column name from measure_names (strip "Sum_" prefix)
    m_name = (measure_names or ["Sum_total_amount"])[0]
    measure_col = m_name[4:] if m_name.startswith("Sum_") else m_name
    d_name = (dim_names or ["category"])[0]

    print(f"\n  Bar chart: category={d_name!r}, measure_col={measure_col!r}")
    (rpt_dir / "report.json").write_text(
        _build_report_json(d_name, measure_col), encoding="utf-8"
    )

    return rpt_dir


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
    assert result.get("fabric") == "ok", f"Auth failed: {result}"
    assert result.get("storage") == "ok", f"Auth failed: {result}"


@pytest.fixture(scope="module")
def sm_artifacts(tmp_path_factory):
    """Build the .SemanticModel folder once; return (path, measure_names, dim_names)."""
    base = tmp_path_factory.mktemp("pbi_deploy")
    return _write_semantic_model(base)


@pytest.fixture(scope="module")
def sm_dir(sm_artifacts):
    return sm_artifacts[0]


@pytest.fixture(scope="module")
def report_dir(tmp_path_factory, sm_artifacts):
    """Build the .Report folder using the actual measure/dim names from the model."""
    _, measure_names, dim_names = sm_artifacts
    base = tmp_path_factory.mktemp("pbi_report")
    return _write_report(base, measure_names, dim_names)


# ── Helper: clean up test items ───────────────────────────────────────────────

def _delete_item_by_name(workspace_id: str, item_type: str, display_name: str):
    """Delete a workspace item by display name (best-effort)."""
    item = _m._find_item_by_name(workspace_id, item_type, display_name)
    if item:
        try:
            _m._fabric_delete(f"workspaces/{workspace_id}/{item_type}/{item['id']}")
        except Exception:
            pass


# ── Tests ──────────────────────────────────────────────────────────────────────

class TestListSemanticModels:
    def test_returns_list(self, workspace_id):
        result = json.loads(_m.list_semantic_models(workspace_id))
        assert isinstance(result, list)

    def test_each_entry_has_id_and_name(self, workspace_id):
        result = json.loads(_m.list_semantic_models(workspace_id))
        for sm in result:
            assert "id" in sm
            assert "displayName" in sm


class TestListReports:
    def test_returns_list(self, workspace_id):
        result = json.loads(_m.list_reports(workspace_id))
        assert isinstance(result, list)

    def test_each_entry_has_id_and_name(self, workspace_id):
        result = json.loads(_m.list_reports(workspace_id))
        for rpt in result:
            assert "id" in rpt
            assert "displayName" in rpt


class TestDeploySemanticModel:
    def test_deploy_creates_model(self, workspace_id, sm_dir):
        # Clean up any leftover from a previous run
        _delete_item_by_name(workspace_id, "semanticModels", SM_NAME)

        result = json.loads(_m.deploy_semantic_model(
            workspace_id=workspace_id,
            display_name=SM_NAME,
            semantic_model_folder=str(sm_dir),
        ))
        print(f"\n  deploy_semantic_model result: {json.dumps(result)}")

        assert "error" not in result, f"deploy failed: {result}"
        assert result.get("displayName") == SM_NAME
        assert result.get("id"), "Expected a non-empty item id"
        assert result.get("created") is True

    def test_deploy_is_idempotent(self, workspace_id, sm_dir):
        """Second deploy of the same name should update (created=False)."""
        result = json.loads(_m.deploy_semantic_model(
            workspace_id=workspace_id,
            display_name=SM_NAME,
            semantic_model_folder=str(sm_dir),
        ))
        print(f"\n  idempotent deploy result: {json.dumps(result)}")

        assert "error" not in result, f"update failed: {result}"
        assert result.get("created") is False

    def test_model_appears_in_list(self, workspace_id):
        models = json.loads(_m.list_semantic_models(workspace_id))
        names = [m["displayName"] for m in models]
        assert SM_NAME in names, f"{SM_NAME} not found in {names}"

    def test_nonexistent_folder_returns_error(self, workspace_id):
        result = json.loads(_m.deploy_semantic_model(
            workspace_id=workspace_id,
            display_name=SM_NAME,
            semantic_model_folder="/nonexistent/folder.SemanticModel",
        ))
        assert "error" in result


class TestRefreshSemanticModel:
    def test_refresh_returns_ok(self, workspace_id):
        """Trigger a framing refresh on the deployed DirectLake model."""
        models = json.loads(_m.list_semantic_models(workspace_id))
        sm = next((m for m in models if m["displayName"] == SM_NAME), None)
        if sm is None:
            pytest.skip(f"{SM_NAME} not found — deploy test may have failed")

        result = json.loads(_m.refresh_semantic_model(
            workspace_id=workspace_id,
            semantic_model_id=sm["id"],
        ))
        print(f"\n  refresh result: {json.dumps(result)}")
        assert result.get("status") == "ok", f"refresh failed: {result}"

    def test_refresh_invalid_id_returns_error(self, workspace_id):
        result = json.loads(_m.refresh_semantic_model(
            workspace_id=workspace_id,
            semantic_model_id="00000000-0000-0000-0000-000000000000",
        ))
        assert result.get("status") == "error"


class TestDeployReport:
    def test_deploy_creates_report(self, workspace_id, report_dir):
        # Resolve semantic model id
        models = json.loads(_m.list_semantic_models(workspace_id))
        sm = next((m for m in models if m["displayName"] == SM_NAME), None)
        if sm is None:
            pytest.skip(f"{SM_NAME} not found — run deploy_semantic_model tests first")

        _delete_item_by_name(workspace_id, "reports", REPORT_NAME)

        result = json.loads(_m.deploy_report(
            workspace_id=workspace_id,
            display_name=REPORT_NAME,
            report_folder=str(report_dir),
            semantic_model_id=sm["id"],
        ))
        print(f"\n  deploy_report result: {json.dumps(result)}")

        assert "error" not in result, f"deploy_report failed: {result}"
        assert result.get("displayName") == REPORT_NAME
        assert result.get("id"), "Expected a non-empty report id"
        assert result.get("created") is True

    def test_deploy_report_is_idempotent(self, workspace_id, report_dir):
        models = json.loads(_m.list_semantic_models(workspace_id))
        sm = next((m for m in models if m["displayName"] == SM_NAME), None)
        if sm is None:
            pytest.skip(f"{SM_NAME} not found")

        result = json.loads(_m.deploy_report(
            workspace_id=workspace_id,
            display_name=REPORT_NAME,
            report_folder=str(report_dir),
            semantic_model_id=sm["id"],
        ))
        print(f"\n  idempotent deploy_report result: {json.dumps(result)}")

        assert "error" not in result, f"update failed: {result}"
        assert result.get("created") is False

    def test_report_appears_in_list(self, workspace_id):
        reports = json.loads(_m.list_reports(workspace_id))
        names = [r["displayName"] for r in reports]
        assert REPORT_NAME in names, f"{REPORT_NAME} not found in {names}"

    def test_nonexistent_folder_returns_error(self, workspace_id):
        result = json.loads(_m.deploy_report(
            workspace_id=workspace_id,
            display_name=REPORT_NAME,
            report_folder="/nonexistent/folder.Report",
            semantic_model_id="00000000-0000-0000-0000-000000000000",
        ))
        assert "error" in result


class TestSemanticModelTables:
    def test_table_is_queryable_after_refresh(self, workspace_id):
        """Verify the DirectLake table is framed and queryable via DAX."""
        models = json.loads(_m.list_semantic_models(workspace_id))
        sm = next((m for m in models if m["displayName"] == SM_NAME), None)
        if sm is None:
            pytest.skip(f"{SM_NAME} not found")

        dax = f"EVALUATE ROW(\"row_count\", COUNTROWS({SOURCE_TABLE}))"
        result = json.loads(_m.execute_dax_query(
            workspace_id=workspace_id,
            semantic_model_id=sm["id"],
            dax=dax,
        ))
        print(f"\n  DAX result: {json.dumps(result)}")

        assert result.get("status") == "ok", f"DAX query failed: {result}"
        rows = result["results"][0]["tables"][0]["rows"]
        row_count = rows[0]["[row_count]"]
        assert row_count > 0, f"Table {SOURCE_TABLE} is empty or not framed"


class TestExportReportPage:
    def test_export_png(self, workspace_id, tmp_path):
        reports = json.loads(_m.list_reports(workspace_id))
        rpt = next((r for r in reports if r["displayName"] == REPORT_NAME), None)
        if rpt is None:
            pytest.skip(f"{REPORT_NAME} not found — run deploy_report tests first")

        out = str(tmp_path / "page1.png")
        result = json.loads(_m.export_report_page(
            workspace_id=workspace_id,
            report_id=rpt["id"],
            page_name="ReportSection",
            output_path=out,
        ))
        print(f"\n  export_report_page result: {json.dumps(result)}")

        assert result.get("status") == "ok", f"export failed: {result}"
        assert result["bytes"] > 0
        assert Path(out).exists()
        # PNG magic bytes: \x89PNG
        assert Path(out).read_bytes()[:4] == b"\x89PNG", "output is not a PNG"

    def test_export_invalid_report_returns_error(self, workspace_id, tmp_path):
        result = json.loads(_m.export_report_page(
            workspace_id=workspace_id,
            report_id="00000000-0000-0000-0000-000000000000",
            page_name="ReportSection",
            output_path=str(tmp_path / "bad.png"),
        ))
        assert result.get("status") == "error"


# ── Cleanup ────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module", autouse=True)
def cleanup(workspace_id, request):
    """Delete test items after the module finishes (skipped with --keep)."""
    yield
    if request.config.getoption("--keep"):
        print(f"\n  --keep: leaving {SM_NAME} and {REPORT_NAME} in workspace.")
        return
    _delete_item_by_name(workspace_id, "reports", REPORT_NAME)
    _delete_item_by_name(workspace_id, "semanticModels", SM_NAME)
    print(f"\n  Cleaned up {SM_NAME} and {REPORT_NAME} from workspace.")
