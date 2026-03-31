"""
SAS Visual Analytics MCP Server
Provides tools for Agent 1 (Documenter) in a SAS VA → Power BI migration pipeline.
"""

import base64
import json
import os
import time
import xml.etree.ElementTree as ET
from typing import Optional
from urllib.parse import urlencode

import httpx
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("SAS Visual Analytics")

# ── Session state (per server process) ────────────────────────────────────────
_session: dict = {"base_url": None, "token": None}

# SAS report XML namespace
SAS_NS = "http://www.sas.com/sasreportmodel/bird-4.50.0"


# ── Internal helpers ───────────────────────────────────────────────────────────

def _require_auth() -> tuple[str, str]:
    if not _session["token"] or not _session["base_url"]:
        raise RuntimeError("Not authenticated. Call `authenticate` first.")
    return _session["base_url"], _session["token"]


def _auth_headers(extra: dict | None = None) -> dict:
    h = {"Authorization": f"Bearer {_session['token']}"}
    if extra:
        h.update(extra)
    return h


def _get(path: str, accept: str = "application/json") -> httpx.Response:
    base_url, _ = _require_auth()
    return httpx.get(
        f"{base_url}{path}",
        headers=_auth_headers({"Accept": accept}),
        verify=False,
        follow_redirects=True,
        timeout=30,
    )


def _post(path: str, body: str, content_type: str, accept: str = "application/json") -> httpx.Response:
    base_url, _ = _require_auth()
    return httpx.post(
        f"{base_url}{path}",
        content=body,
        headers=_auth_headers({"Content-Type": content_type, "Accept": accept}),
        verify=False,
        follow_redirects=True,
        timeout=60,
    )


def _poll_image_job(job_id: str, max_attempts: int = 30) -> dict:
    for _ in range(max_attempts):
        r = _get(f"/reportImages/jobs/{job_id}", accept="application/vnd.sas.report.images.job+json")
        data = r.json()
        if data["state"] in ("completed", "failed"):
            return data
        time.sleep(2)
    raise TimeoutError(f"Image job {job_id} did not complete in {max_attempts * 2}s")


def _fetch_report_xml(report_id: str) -> ET.Element:
    r = _get(f"/reports/reports/{report_id}/content", accept="application/vnd.sas.report.content+xml")
    r.raise_for_status()
    return ET.fromstring(r.text)


def _tag(name: str) -> str:
    """Fully-qualified XML tag with SAS namespace."""
    return f"{{{SAS_NS}}}{name}"


def _attrib(el: ET.Element, key: str, default: str = "") -> str:
    return el.attrib.get(key, default)


# ── Tool 1: Authentication ─────────────────────────────────────────────────────

@mcp.tool()
def authenticate(base_url: str, username: str, password: str) -> dict:
    """
    Authenticate to SAS Viya using username/password credentials.
    Stores the access token in server memory for all subsequent tool calls.
    Must be called before any other tool.

    Args:
        base_url: Server root, e.g. 'https://myserver.example.com'
        username: SAS Viya username
        password: SAS Viya password
    """
    body = urlencode({
        "grant_type": "password",
        "username": username,
        "password": password,
        "client_id": "sas.ec",
        "client_secret": "",
    })
    r = httpx.post(
        f"{base_url.rstrip('/')}/SASLogon/oauth/token",
        content=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        verify=False,
        follow_redirects=True,
        timeout=30,
    )
    if r.status_code != 200:
        return {"error": f"Authentication failed ({r.status_code}): {r.text[:500]}"}

    data = r.json()
    _session["base_url"] = base_url.rstrip("/")
    _session["token"] = data["access_token"]

    return {
        "authenticated": True,
        "username": username,
        "base_url": _session["base_url"],
        "token_type": data.get("token_type"),
        "expires_in": data.get("expires_in"),
        "scope": data.get("scope"),
    }


# ── Tool 2: List reports ───────────────────────────────────────────────────────

@mcp.tool()
def list_reports(limit: int = 20, filter_text: str = "") -> dict:
    """
    List SAS Visual Analytics reports available on the server.

    Args:
        limit: Maximum number of reports to return (default 20, max 100)
        filter_text: Optional text to filter report names (case-insensitive contains)
    """
    params = f"?limit={min(limit, 100)}&sortBy=name"
    if filter_text:
        params += f"&filter=contains(name,'{filter_text}')"

    r = _get(f"/reports/reports{params}", accept="application/vnd.sas.collection+json")
    r.raise_for_status()
    data = r.json()

    reports = [
        {
            "id": item.get("id"),
            "name": item.get("name"),
            "description": item.get("description", ""),
            "createdBy": item.get("createdBy"),
            "creationTimeStamp": item.get("creationTimeStamp"),
            "modifiedTimeStamp": item.get("modifiedTimeStamp"),
        }
        for item in data.get("items", [])
    ]

    return {"count": data.get("count", len(reports)), "reports": reports}


# ── Tool 3: Report metadata ────────────────────────────────────────────────────

@mcp.tool()
def get_report_metadata(report_id: str) -> dict:
    """
    Get metadata for a specific report: name, description, ownership, timestamps, tags.

    Args:
        report_id: The report UUID (e.g. 'cbf97b0a-457d-4b4f-8913-547e0cdf390c')
    """
    r = _get(f"/reports/reports/{report_id}", accept="application/vnd.sas.report+json")
    r.raise_for_status()
    d = r.json()

    return {
        "id": d.get("id"),
        "name": d.get("name"),
        "description": d.get("description", ""),
        "createdBy": d.get("createdBy"),
        "creationTimeStamp": d.get("creationTimeStamp"),
        "modifiedBy": d.get("modifiedBy"),
        "modifiedTimeStamp": d.get("modifiedTimeStamp"),
        "imageUri": d.get("imageUri"),
    }


# ── Tool 4: Report sections (pages) ───────────────────────────────────────────

@mcp.tool()
def get_report_sections(report_id: str, include_hidden: bool = False) -> dict:
    """
    List the sections (pages/tabs) of a report.

    Args:
        report_id: The report UUID
        include_hidden: Include hidden info sections (default False)
    """
    r = _get(
        f"/reports/reports/{report_id}/content/elements",
        accept="application/vnd.sas.collection+json",
    )
    r.raise_for_status()
    data = r.json()

    section_types = {"Section", "HiddenSection"} if include_hidden else {"Section"}
    sections = [
        {
            "index": i,
            "name": item.get("name"),
            "label": item.get("label", ""),
            "type": item.get("type"),
            "hidden": item.get("hidden", False),
        }
        for i, item in enumerate(
            [el for el in data.get("items", []) if el.get("type") in section_types]
        )
    ]

    return {"report_id": report_id, "section_count": len(sections), "sections": sections}


# ── Tool 5: All report elements ────────────────────────────────────────────────

@mcp.tool()
def get_report_elements(report_id: str, element_types: str = "") -> dict:
    """
    Get all elements in a report, optionally filtered by type.

    Args:
        report_id: The report UUID
        element_types: Comma-separated element types to include. Leave empty for all.
            Common types: Section, HiddenSection, Graph, Table, Crosstab, Text, Prompt,
            DataSource, DataItem, AggregateCalculatedItem, CalculatedItem, Hierarchy,
            VisualContainer, PromptDefinition, NavigationAction
    """
    r = _get(
        f"/reports/reports/{report_id}/content/elements",
        accept="application/vnd.sas.collection+json",
    )
    r.raise_for_status()
    data = r.json()

    items = data.get("items", [])
    if element_types:
        wanted = {t.strip() for t in element_types.split(",")}
        items = [el for el in items if el.get("type") in wanted]

    return {
        "report_id": report_id,
        "total_elements": data.get("count", len(items)),
        "returned": len(items),
        "elements": items,
    }


# ── Tool 6: Raw XML content ────────────────────────────────────────────────────

@mcp.tool()
def get_report_content_xml(report_id: str) -> dict:
    """
    Retrieve the raw SASReport XML definition of a report.
    Useful for deep analysis. Returns the XML as a string.

    Args:
        report_id: The report UUID
    """
    r = _get(f"/reports/reports/{report_id}/content", accept="application/vnd.sas.report.content+xml")
    r.raise_for_status()
    return {"report_id": report_id, "content_type": "application/vnd.sas.report.content+xml", "xml": r.text}


# ── Tool 7: Parse data sources ─────────────────────────────────────────────────

@mcp.tool()
def parse_data_sources(report_id: str) -> dict:
    """
    Parse the report XML and extract structured data source information:
    CAS server/library/table connections and their column definitions.

    Args:
        report_id: The report UUID
    """
    root = _fetch_report_xml(report_id)
    sources = []

    for ds in root.iter(_tag("DataSource")):
        source: dict = {
            "name": _attrib(ds, "name"),
            "label": _attrib(ds, "label"),
            "type": _attrib(ds, "type"),
            "connection": {},
            "columns": [],
            "calculations": [],
            "hierarchies": [],
        }

        # CAS connection details
        cas = ds.find(_tag("CasResource"))
        if cas is not None:
            source["connection"] = {
                "server": _attrib(cas, "server"),
                "library": _attrib(cas, "library"),
                "table": _attrib(cas, "table"),
                "locale": _attrib(cas, "locale"),
            }

        # Esri map provider
        esri = ds.find(_tag("EsriMapProvider"))
        if esri is not None:
            source["connection"] = {"type": "EsriMapProvider", **esri.attrib}

        # First pass: build bi-name → {xref, label} lookup for all DataItems
        bi_lookup: dict[str, dict] = {}
        for folder in ds.iter(_tag("BusinessItemFolder")):
            for child in folder:
                tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                if tag == "DataItem":
                    bi_name = _attrib(child, "name")
                    bi_lookup[bi_name] = {
                        "xref": _attrib(child, "xref"),
                        "label": _attrib(child, "label"),
                    }

        # Second pass: collect columns, calculations, and hierarchies
        for folder in ds.iter(_tag("BusinessItemFolder")):
            for child in folder:
                tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                item: dict = {
                    "name": _attrib(child, "name"),
                    "label": _attrib(child, "label"),
                    "type": tag,
                    "xref": _attrib(child, "xref"),
                }

                if tag in ("AggregateCalculatedItem", "CalculatedItem"):
                    expr = child.find(_tag("Expression"))
                    item["expression"] = expr.text if expr is not None else ""
                    source["calculations"].append(item)
                elif tag == "Hierarchy":
                    # Extract ordered levels, resolving each Level ref to its column xref
                    levels = []
                    for level_el in child:
                        level_tag = level_el.tag.split("}")[-1] if "}" in level_el.tag else level_el.tag
                        if level_tag == "Level":
                            ref = _attrib(level_el, "ref")
                            resolved = bi_lookup.get(ref, {})
                            levels.append({
                                "bi_name": ref,
                                "xref": resolved.get("xref", ""),
                                "label": resolved.get("label", ref),
                            })
                    item["levels"] = levels
                    source["hierarchies"].append(item)
                elif tag == "DataItem":
                    source["columns"].append(item)

        sources.append(source)

    return {"report_id": report_id, "data_source_count": len(sources), "data_sources": sources}


# ── Tool 8: Parse calculations ─────────────────────────────────────────────────

@mcp.tool()
def parse_calculations(report_id: str) -> dict:
    """
    Extract all calculated measures and derived items from the report XML.
    Captures expressions, aggregation types, and grouping logic.

    Args:
        report_id: The report UUID
    """
    root = _fetch_report_xml(report_id)
    calc_tags = {
        "AggregateCalculatedItem", "CalculatedItem",
        "GroupedItem", "SourcePredefinedDataItem",
    }
    seen = set()
    calculations = []

    for tag_name in calc_tags:
        for el in root.iter(_tag(tag_name)):
            name = _attrib(el, "name")
            if name in seen:
                continue
            seen.add(name)

            calc: dict = {
                "name": name,
                "label": _attrib(el, "label"),
                "type": tag_name,
                "expression": "",
                "classification": _attrib(el, "classification"),
                "aggregation": _attrib(el, "aggregation"),
            }

            expr = el.find(_tag("Expression"))
            if expr is not None:
                calc["expression"] = (expr.text or "").strip()

            # GroupedItem: capture bin definitions
            bins = []
            for bin_el in el.iter(_tag("BinItem")):
                bins.append({
                    "label": _attrib(bin_el, "label"),
                    "lowerBound": _attrib(bin_el, "lowerBound"),
                    "upperBound": _attrib(bin_el, "upperBound"),
                    "value": _attrib(bin_el, "value"),
                })
            if bins:
                calc["bins"] = bins

            calculations.append(calc)

    return {
        "report_id": report_id,
        "calculation_count": len(calculations),
        "calculations": calculations,
    }


# ── Tool 9: Parse visual elements ─────────────────────────────────────────────

@mcp.tool()
def parse_visual_elements(report_id: str) -> dict:
    """
    Parse visual elements (graphs, tables, crosstabs, text, prompts) from the report XML.
    For each visual, extracts its type, section membership, and data item references.

    Args:
        report_id: The report UUID
    """
    root = _fetch_report_xml(report_id)
    visual_tags = {"Graph", "Table", "Crosstab", "Text", "Prompt"}
    visuals = []

    for tag_name in visual_tags:
        for el in root.iter(_tag(tag_name)):
            vis: dict = {
                "name": _attrib(el, "name"),
                "label": _attrib(el, "label"),
                "type": tag_name,
                "data_definition": _attrib(el, "data") or _attrib(el, "dataDefinition"),
                "graph_type": _attrib(el, "graphType") if tag_name == "Graph" else None,
                "result_definitions": _attrib(el, "resultDefinitions"),
                "referenced_data_items": [],
            }

            # Collect all BusinessItem refs within this visual
            refs = set()
            for ref_el in el.iter(_tag("BusinessItem")):
                refs.add(_attrib(ref_el, "ref"))
            for role_el in el.iter(_tag("Role")):
                refs.add(_attrib(role_el, "ref"))
            vis["referenced_data_items"] = sorted(r for r in refs if r)

            # Text content
            if tag_name == "Text":
                spans = [sp.text or "" for sp in el.iter(_tag("Span"))]
                vis["text_content"] = " ".join(spans).strip()

            # Prompt type
            if tag_name == "Prompt":
                vis["prompt_definition"] = _attrib(el, "promptDefinition")

            visuals.append(vis)

    return {
        "report_id": report_id,
        "visual_count": len(visuals),
        "visuals": visuals,
    }


# ── Tool 10: Parse filters and prompts ────────────────────────────────────────

@mcp.tool()
def parse_filters_and_prompts(report_id: str) -> dict:
    """
    Extract filter definitions (detail/post-processing filters, rank/top-N)
    and interactive prompt (parameter) definitions from the report XML.

    Args:
        report_id: The report UUID
    """
    root = _fetch_report_xml(report_id)
    filters = []
    prompts = []

    # Filters from DataDefinitions
    for pdd in root.iter(_tag("ParentDataDefinition")):
        pdd_name = _attrib(pdd, "name")
        pdd_ds = _attrib(pdd, "dataSource")

        # Detail filters (row-level)
        for df in pdd.iter(_tag("DetailFilters")):
            for bi in df.iter(_tag("BusinessItem")):
                filters.append({
                    "parent_definition": pdd_name,
                    "data_source": pdd_ds,
                    "filter_type": "detail",
                    "business_item_ref": _attrib(bi, "ref"),
                })

        # Post-processing filters (aggregate)
        for pf in pdd.iter(_tag("PostProcessingFilters")):
            for bi in pf.iter(_tag("BusinessItem")):
                filters.append({
                    "parent_definition": pdd_name,
                    "data_source": pdd_ds,
                    "filter_type": "post_processing",
                    "business_item_ref": _attrib(bi, "ref"),
                })

        # Rank/Top-N items
        for rank in pdd.iter(_tag("RankItem")):
            for level in rank.iter(_tag("RankLevel")):
                filters.append({
                    "parent_definition": pdd_name,
                    "data_source": pdd_ds,
                    "filter_type": "rank",
                    "rank_by": _attrib(rank, "rankBy"),
                    "group_by": _attrib(rank, "groupBy"),
                    "n": _attrib(level, "n"),
                    "subset": _attrib(level, "subset"),
                    "include_ties": _attrib(level, "includeTies"),
                })

    # Prompt definitions
    for pd in root.iter(_tag("PromptDefinition")):
        prompt: dict = {
            "name": _attrib(pd, "name"),
            "label": _attrib(pd, "label"),
            "type": _attrib(pd, "type"),
        }
        # Collect constraint info
        constraint = pd.find(_tag("Constraint"))
        if constraint is not None:
            prompt["constraint"] = {
                "data_item": _attrib(constraint, "dataItem"),
                "operator": _attrib(constraint, "operator"),
            }
        prompts.append(prompt)

    # RelationalFilterItem expressions (inline filter logic)
    filter_expressions = []
    seen_filters = set()
    for rfi in root.iter(_tag("RelationalFilterItem")):
        name = _attrib(rfi, "name")
        if name in seen_filters:
            continue
        seen_filters.add(name)
        expr = rfi.find(_tag("Expression"))
        if expr is not None and expr.text:
            filter_expressions.append({
                "name": name,
                "expression": expr.text.strip(),
            })

    return {
        "report_id": report_id,
        "filter_count": len(filters),
        "filters": filters,
        "prompt_count": len(prompts),
        "prompts": prompts,
        "filter_expression_count": len(filter_expressions),
        "filter_expressions": filter_expressions,
    }


# ── Tool 11: Screenshot of one section ────────────────────────────────────────

def _visible_to_absolute_section_index(report_id: str, visible_index: int) -> int:
    """
    The reportImages API uses sectionIndex counting ALL sections (visible + hidden).
    This converts a zero-based visible-section index to the absolute index needed by the API.
    """
    r = _get(
        f"/reports/reports/{report_id}/content/elements",
        accept="application/vnd.sas.collection+json",
    )
    r.raise_for_status()
    items = r.json().get("items", [])
    # Walk all Section/HiddenSection elements in order, tracking absolute position
    abs_pos = 0
    visible_count = 0
    for item in items:
        if item.get("type") in ("Section", "HiddenSection"):
            if item.get("type") == "Section":
                if visible_count == visible_index:
                    return abs_pos
                visible_count += 1
            abs_pos += 1
    raise ValueError(f"Visible section index {visible_index} not found (only {visible_count} visible sections)")


@mcp.tool()
def get_section_screenshot(
    report_id: str,
    section_index: int = 0,
    width: int = 1920,
    height: int = 1080,
) -> dict:
    """
    Get a PNG screenshot of a specific report section, returned as a base64 string.
    section_index is the zero-based index among VISIBLE sections only
    (as returned by get_report_sections).

    Args:
        report_id: The report UUID
        section_index: Zero-based visible-section index (0 = first visible page)
        width: Image width in pixels (default 1920)
        height: Image height in pixels (default 1080)
    """
    # Resolve to the absolute sectionIndex the API expects (counts hidden sections too)
    try:
        abs_index = _visible_to_absolute_section_index(report_id, section_index)
    except ValueError as e:
        return {"error": str(e)}

    payload = json.dumps({
        "reportUri": f"/reports/reports/{report_id}",
        "layoutType": "entireSection",
        "selectionType": "report",
        "size": f"{width}x{height}",
        "imageType": "png",
        "sectionIndex": abs_index,
    })

    r = _post(
        "/reportImages/jobs",
        payload,
        content_type="application/vnd.sas.report.images.job.request+json",
        accept="application/vnd.sas.report.images.job+json",
    )
    if r.status_code not in (200, 201, 202):
        return {"error": f"Job creation failed ({r.status_code}): {r.text[:500]}"}

    job = r.json()
    if job["state"] not in ("completed", "failed"):
        job = _poll_image_job(job["id"])

    if job["state"] == "failed":
        return {"error": "Image job failed", "job": job}

    images = job.get("images", [])
    if not images:
        return {"error": "No images returned by job", "job_state": job.get("state")}

    img_meta = images[0]  # job for a single sectionIndex always returns 1 image
    link = next((l for l in img_meta.get("links", []) if l.get("rel") == "image"), None)
    if not link:
        return {"error": "No image link in job response", "image_meta": img_meta}

    img_r = _get(link["href"], accept="image/png")
    img_r.raise_for_status()

    return {
        "report_id": report_id,
        "section_index": section_index,
        "section_name": img_meta.get("sectionName"),
        "section_label": img_meta.get("sectionLabel"),
        "size": f"{width}x{height}",
        "mime_type": "image/png",
        "base64_png": base64.b64encode(img_r.content).decode(),
        "bytes": len(img_r.content),
    }


# ── Tool 12: Save screenshot to disk ──────────────────────────────────────────

@mcp.tool()
def save_section_screenshot(
    report_id: str,
    section_index: int = 0,
    output_path: str = "",
    width: int = 1920,
    height: int = 1080,
) -> dict:
    """
    Capture a PNG screenshot of a report section and save it to disk.

    Args:
        report_id: The report UUID
        section_index: Zero-based section index
        output_path: Full file path to save PNG (auto-generated if empty)
        width: Image width in pixels (default 1920)
        height: Image height in pixels (default 1080)
    """
    result = get_section_screenshot(report_id, section_index, width, height)
    if "error" in result:
        return result

    if not output_path:
        safe_label = (result["section_label"] or result["section_name"] or f"section_{section_index}")
        safe_label = "".join(c if c.isalnum() or c in "_- " else "_" for c in safe_label).strip()
        output_path = f"{report_id}_{safe_label}.png"

    img_bytes = base64.b64decode(result["base64_png"])
    with open(output_path, "wb") as f:
        f.write(img_bytes)

    return {
        "saved": True,
        "output_path": os.path.abspath(output_path),
        "section_label": result["section_label"],
        "bytes": result["bytes"],
    }


# ── Tool 13: Screenshot all sections ──────────────────────────────────────────

@mcp.tool()
def save_all_screenshots(
    report_id: str,
    output_dir: str = ".",
    width: int = 1920,
    height: int = 1080,
) -> dict:
    """
    Capture and save PNG screenshots of all visible sections in a report.

    Args:
        report_id: The report UUID
        output_dir: Directory to save PNGs into (default: current directory)
        width: Image width in pixels (default 1920)
        height: Image height in pixels (default 1080)
    """
    os.makedirs(output_dir, exist_ok=True)

    sections_result = get_report_sections(report_id, include_hidden=False)
    sections = sections_result.get("sections", [])

    saved = []
    errors = []

    for section in sections:
        idx = section["index"]
        safe_label = (section["label"] or section["name"] or f"section_{idx}")
        safe_label = "".join(c if c.isalnum() or c in "_- " else "_" for c in safe_label).strip()
        out_path = os.path.join(output_dir, f"{idx:02d}_{safe_label}.png")

        result = save_section_screenshot(report_id, idx, out_path, width, height)
        if "error" in result:
            errors.append({"section_index": idx, "error": result["error"]})
        else:
            saved.append({
                "section_index": idx,
                "section_label": section["label"],
                "output_path": result["output_path"],
                "bytes": result["bytes"],
            })

    return {
        "report_id": report_id,
        "saved_count": len(saved),
        "error_count": len(errors),
        "saved": saved,
        "errors": errors,
    }


# ── Tool 14: Full report documentation bundle ─────────────────────────────────

@mcp.tool()
def document_report(report_id: str, output_dir: str = ".") -> dict:
    """
    Run a full documentation pass on a report and write all artifacts to output_dir:
      - metadata.json      — report name, owner, timestamps
      - sections.json      — page/section list
      - data_sources.json  — CAS tables, columns
      - calculations.json  — all calculated measures and derived items
      - visuals.json       — visual elements with data bindings
      - filters.json       — filters, prompts, rank conditions
      - screenshots/       — PNG per visible section

    Returns a summary of what was written.

    Args:
        report_id: The report UUID
        output_dir: Root directory for output (created if absent)
    """
    os.makedirs(output_dir, exist_ok=True)
    screenshots_dir = os.path.join(output_dir, "screenshots")
    written = []

    def _write(filename: str, data: dict) -> str:
        path = os.path.join(output_dir, filename)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        written.append({"file": path, "keys": list(data.keys())})
        return path

    metadata    = get_report_metadata(report_id)
    sections    = get_report_sections(report_id, include_hidden=True)
    elements    = get_report_elements(report_id)
    data_srcs   = parse_data_sources(report_id)
    calcs       = parse_calculations(report_id)
    visuals     = parse_visual_elements(report_id)
    filters     = parse_filters_and_prompts(report_id)
    screenshots = save_all_screenshots(report_id, screenshots_dir)

    _write("metadata.json", metadata)
    _write("sections.json", sections)
    _write("elements.json", elements)
    _write("data_sources.json", data_srcs)
    _write("calculations.json", calcs)
    _write("visuals.json", visuals)
    _write("filters.json", filters)
    _write("screenshots_manifest.json", screenshots)

    return {
        "report_id": report_id,
        "report_name": metadata.get("name"),
        "output_dir": os.path.abspath(output_dir),
        "files_written": written,
        "sections": sections.get("section_count"),
        "data_sources": data_srcs.get("data_source_count"),
        "calculations": calcs.get("calculation_count"),
        "visuals": visuals.get("visual_count"),
        "screenshots_saved": screenshots.get("saved_count"),
    }


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    mcp.run()
