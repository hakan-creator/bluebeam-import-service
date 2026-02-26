from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
import os, requests, xml.etree.ElementTree as ET, zlib, binascii, re
from typing import Optional, List

app = FastAPI(title="Bluebeam BPX Import Service")

IMPORT_API_KEY = os.environ.get("IMPORT_SERVICE_API_KEY", "")


class ImportReq(BaseModel):
    project_id: Optional[str] = None
    storage_bucket: str = "imports"
    storage_path: str
    filename: str = "unknown.bpx"
    supabase_url: str
    supabase_service_role_key: str


def sb_headers(service_key: str):
    return {
        "apikey": service_key,
        "Authorization": f"Bearer {service_key}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }


def decode_hex_zlib(hex_str: str) -> str:
    hex_str = (hex_str or "").strip()
    if not hex_str:
        return ""
    raw = binascii.unhexlify(hex_str)
    out = zlib.decompress(raw)
    return out.decode("utf-8", errors="replace")


def extract_pdf_dict_fields(raw: str):
    subj = None
    m = re.search(r"/Subj\((.*?)\)", raw)
    if m:
        subj = m.group(1)

    it = None
    m = re.search(r"/IT/([A-Za-z0-9]+)", raw)
    if m:
        it = m.group(1)

    def extract_array(key):
        m2 = re.search(rf"/{key}\s*\[\s*([0-9.\s]+)\]", raw)
        if not m2:
            return None
        return [float(x) for x in m2.group(1).split() if x.strip()]

    def extract_num(key):
        m2 = re.search(rf"/{key}\s+([0-9.]+)", raw)
        return float(m2.group(1)) if m2 else None

    style = {}
    C = extract_array("C")
    IC = extract_array("IC")
    CA = extract_num("CA")
    LW = extract_num("LW")

    md = re.search(r"/D\s*\[\s*([0-9.\s]+)\]", raw)
    D = [float(x) for x in md.group(1).split() if x.strip()] if md else None

    if C:
        style["stroke_rgb"] = C[:3]
    if IC:
        style["fill_rgb"] = IC[:3]
    if CA is not None:
        style["opacity"] = CA
    if LW is not None:
        style["line_width"] = LW
    if D:
        style["dash"] = D

    return subj, it, style


TYPE_MAP = {
    "annotationpolyline": "length",
    "annotationline": "length",
    "annotationlength": "length",
    "annotationmeasureperimeter": "length",
    "annotationpolygon": "area",
    "annotationarea": "area",
    "annotationrectangle": "area",
    "annotationsquare": "area",
    "annotationcount": "count",
    "annotationcloudplus": "count",
}

SKIP_TYPES = {
    "annotationbrxstamp",
    "annotationcircle",
    "annotationimage",
    "annotationstamp",
    "annotationfreetext",
    "annotationcallout",
}


def map_tool_kind(it: Optional[str]) -> str:
    if not it:
        return "count"
    short = it.split(".")[-1].lower()
    if short in SKIP_TYPES:
        return "skip"
    return TYPE_MAP.get(short, "count")


def sb_download(supabase_url: str, service_key: str, bucket: str, path: str) -> str:
    """Download file from Supabase Storage via signed URL."""
    url = f"{supabase_url}/storage/v1/object/sign/{bucket}/{path}"
    r = requests.post(url, headers=sb_headers(service_key), json={"expiresIn": 300})
    if r.status_code >= 300:
        raise RuntimeError(f"Failed signed URL: {r.status_code} {r.text}")
    signed = r.json()["signedURL"]
    file_url = f"{supabase_url}{signed}" if signed.startswith("/") else signed
    fr = requests.get(file_url)
    if fr.status_code >= 300:
        raise RuntimeError(f"Failed to download file: {fr.status_code}")
    return fr.text


def sb_insert(supabase_url: str, service_key: str, table: str, row: dict) -> dict:
    """Insert a row and return it (with generated id)."""
    url = f"{supabase_url}/rest/v1/{table}"
    r = requests.post(url, headers=sb_headers(service_key), json=row)
    if r.status_code >= 300:
        raise RuntimeError(f"Insert {table} failed: {r.status_code} {r.text}")
    data = r.json()
    return data[0] if isinstance(data, list) and data else data


def sb_delete_where(supabase_url: str, service_key: str, table: str, where: str):
    url = f"{supabase_url}/rest/v1/{table}?{where}"
    r = requests.delete(url, headers=sb_headers(service_key))
    # Ignore 404/empty — fine if nothing to delete


@app.post("/import-bpx")
def import_bpx(req: ImportReq, authorization: str = Header(default="")):
    if not IMPORT_API_KEY or authorization != f"Bearer {IMPORT_API_KEY}":
        raise HTTPException(status_code=401, detail="Unauthorized")

    base = req.supabase_url
    key = req.supabase_service_role_key

    # 1. Download BPX from Storage
    xml_text = sb_download(base, key, req.storage_bucket, req.storage_path)

    # 2. Create bluebeam_profile
    profile = sb_insert(base, key, "bluebeam_profiles", {
        "project_id": req.project_id,
        "filename": req.filename,
        "version_label": None,
        "created_by": "00000000-0000-0000-0000-000000000000",  # service-level
    })
    profile_id = profile["id"]

    # 3. Parse BPX XML
    root = ET.fromstring(xml_text)
    toolsets_imported = 0
    tools_imported = 0
    presets_created = 0
    warnings: List[str] = []

    toolset_els = root.findall(".//BluebeamRevuToolSet")

    for ts_idx, ts in enumerate(toolset_els):
        title_hex = ts.findtext("Title") or ""
        try:
            title = decode_hex_zlib(title_hex) if title_hex else f"Toolset {ts_idx + 1}"
        except Exception:
            title = f"Toolset {ts_idx + 1}"
            warnings.append(f"Failed decoding toolset title at index {ts_idx}")

        if title in ("Recent Tools", "Seneste vaerktoejer"):
            continue

        ts_row = sb_insert(base, key, "bluebeam_toolsets", {
            "profile_id": profile_id,
            "title": title,
            "sort_index": ts_idx,
            "source_path": None,
        })
        toolset_id = ts_row["id"]
        toolsets_imported += 1

        items = ts.findall(".//ToolChestItem")
        for i, item in enumerate(items):
            raw_hex = item.findtext("Raw") or ""
            if not raw_hex:
                continue
            try:
                raw = decode_hex_zlib(raw_hex)
            except Exception:
                warnings.append(f"Failed decoding tool in '{title}', item {i}")
                continue

            subj, it, style = extract_pdf_dict_fields(raw)
            tool_kind = map_tool_kind(it)
            if tool_kind == "skip":
                continue

            name = subj or f"Tool {i + 1}"

            tool_row = sb_insert(base, key, "bluebeam_tools", {
                "toolset_id": toolset_id,
                "name": name,
                "tool_kind": tool_kind,
                "sort_index": i,
                "raw_decoded": raw[:4000],
                "style_json": style,
                "mapping_json": {"it": it, "category": title},
            })
            tools_imported += 1

            # Create preset
            preset = {
                "project_id": req.project_id,
                "name": name,
                "tool_type": tool_kind,
                "category": title,
                "style_json": style,
                "default_tags_json": {},
                "sort_index": ts_idx * 1000 + i,
                "bluebeam_tool_id": tool_row["id"],
            }
            try:
                sb_insert(base, key, "presets", preset)
                presets_created += 1
            except Exception as e:
                warnings.append(f"Failed preset for '{name}': {str(e)}")

    return {
        "profileId": profile_id,
        "toolsetCount": toolsets_imported,
        "toolCount": tools_imported,
        "presetCount": presets_created,
        "missingReferences": [],
        "warnings": warnings,
    }
