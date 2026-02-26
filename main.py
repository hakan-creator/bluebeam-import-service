from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
import os, requests, xml.etree.ElementTree as ET, zlib, binascii, re
from typing import Optional

app = FastAPI(title="Bluebeam BPX Import Service")

IMPORT_API_KEY = os.environ.get("IMPORT_SERVICE_API_KEY", "")
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

class ImportReq(BaseModel):
    project_id: str
    profile_id: str
    storage_bucket: str = "imports"
    storage_path: str

def sb_headers():
    return {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
    }

def decode_hex_zlib(hex_str: str) -> str:
    hex_str = (hex_str or "").strip()
    if not hex_str:
        return ""
    raw = binascii.unhexlify(hex_str)
    out = zlib.decompress(raw)
    return out.decode("utf-8", errors="replace")

def extract_pdf_dict_fields(raw: str):
    # Minimal extraction: Subj + IT + color/opacity/line width/dash
    subj = None
    m = re.search(r"/Subj\((.*?)\)", raw)
    if m: subj = m.group(1)

    it = None
    m = re.search(r"/IT/([A-Za-z0-9]+)", raw)
    if m: it = m.group(1)

    def extract_array(key):
        m = re.search(rf"/{key}\s*\[\s*([0-9\.\s]+)\]", raw)
        if not m: 
            return None
        vals = [float(x) for x in m.group(1).split() if x.strip()]
        return vals

    def extract_num(key):
        m = re.search(rf"/{key}\s+([0-9\.]+)", raw)
        return float(m.group(1)) if m else None

    style = {}
    C = extract_array("C")   # stroke RGB 0..1
    IC = extract_array("IC") # fill RGB 0..1
    CA = extract_num("CA")   # opacity 0..1
    LW = extract_num("LW")   # line width
    D = None
    md = re.search(r"/D\s*\[\s*([0-9\.\s]+)\]", raw)  # dash array sometimes
    if md:
        D = [float(x) for x in md.group(1).split() if x.strip()]

    if C: style["stroke_rgb"] = C[:3]
    if IC: style["fill_rgb"] = IC[:3]
    if CA is not None: style["opacity"] = CA
    if LW is not None: style["line_width"] = LW
    if D: style["dash"] = D

    return subj, it, style

def map_tool_kind(it: Optional[str]) -> str:
    if not it:
        return "count"
    s = it.lower()
    if "polyline" in s:
        return "length"
    if "polygon" in s:
        return "area"
    return "count"

def supabase_download_signed_url(bucket: str, path: str, expires_in: int = 300) -> str:
    url = f"{SUPABASE_URL}/storage/v1/object/sign/{bucket}/{path}"
    r = requests.post(url, headers=sb_headers(), json={"expiresIn": expires_in})
    if r.status_code >= 300:
        raise RuntimeError(f"Failed signed URL: {r.status_code} {r.text}")
    return r.json()["signedURL"]

def supabase_insert(table: str, rows):
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    r = requests.post(url, headers=sb_headers(), json=rows)
    if r.status_code >= 300:
        raise RuntimeError(f"Insert {table} failed: {r.status_code} {r.text}")
    return r.json() if r.text else None

def supabase_delete_where(table: str, where: str):
    url = f"{SUPABASE_URL}/rest/v1/{table}?{where}"
    r = requests.delete(url, headers=sb_headers())
    if r.status_code >= 300:
        raise RuntimeError(f"Delete {table} failed: {r.status_code} {r.text}")

@app.post("/import-bpx")
def import_bpx(req: ImportReq, authorization: str = Header(default="")):
    if not IMPORT_API_KEY or authorization != f"Bearer {IMPORT_API_KEY}":
        raise HTTPException(status_code=401, detail="Unauthorized")

    # Download BPX from Supabase Storage via signed URL
    signed = supabase_download_signed_url(req.storage_bucket, req.storage_path, 300)
    file_url = f"{SUPABASE_URL}{signed}" if signed.startswith("/") else signed
    fr = requests.get(file_url)
    if fr.status_code >= 300:
        raise HTTPException(status_code=400, detail=f"Failed to download BPX: {fr.text}")
    xml_text = fr.text

    # OPTIONAL: clear previous toolsets/tools for this profile (re-import)
    # This relies on FK cascade: delete toolsets by profile_id -> tools cascade.
    supabase_delete_where("bluebeam_toolsets", f"profile_id=eq.{req.profile_id}")

    root = ET.fromstring(xml_text)
    ns = ""  # BPX often has no namespaces; keep simple

    toolsets = root.findall(".//BluebeamRevuToolSet")
    toolsets_imported = 0
    tools_imported = 0
    presets_created = 0
    warnings = []

    for ts_idx, ts in enumerate(toolsets):
        title_hex = ts.findtext("Title") or ""
        try:
            title = decode_hex_zlib(title_hex) if title_hex else f"Toolset {ts_idx+1}"
        except Exception:
            title = f"Toolset {ts_idx+1}"
            warnings.append(f"Failed decoding toolset title at index {ts_idx}")

        toolset_row = {
            "profile_id": req.profile_id,
            "title": title,
            "sort_index": ts_idx,
            "source_path": None,
        }
        supabase_insert("bluebeam_toolsets", toolset_row)
        # Fetch created toolset id: simplest is to query back; but we can store via returning=representation if needed.
        # For simplicity, re-select by (profile_id,title,sort_index)
        q = requests.get(
            f"{SUPABASE_URL}/rest/v1/bluebeam_toolsets"
            f"?profile_id=eq.{req.profile_id}&sort_index=eq.{ts_idx}&select=id",
            headers=sb_headers(),
        )
        toolset_id = q.json()[0]["id"]

        toolsets_imported += 1

        items = ts.findall(".//ToolChestItem")
        for i, item in enumerate(items):
            raw_hex = item.findtext("Raw") or ""
            if not raw_hex:
                continue
            try:
                raw = decode_hex_zlib(raw_hex)
            except Exception:
                warnings.append(f"Failed decoding tool raw in toolset '{title}', item {i}")
                continue

            subj, it, style = extract_pdf_dict_fields(raw)
            name = subj or f"Tool {i+1}"
            tool_kind = map_tool_kind(it)

            tool_row = {
                "toolset_id": toolset_id,
                "name": name,
                "tool_kind": tool_kind,
                "sort_index": i,
                "raw_decoded": raw,       # keep for debug (can remove later)
                "style_json": style,
                "mapping_json": {"it": it, "source": "bpx"},
            }
            supabase_insert("bluebeam_tools", tool_row)
            tools_imported += 1

            # Also create a preset in our system (Tool Chest)
            preset_row = {
                "project_id": req.project_id,
                "name": name,
                "tool_type": tool_kind,
                "category": title,
                "style_json": {**style, "imported_from": "bluebeam"},
                "default_tags_json": {"imported_from": "bpx", "toolset": title},
                "sort_index": i,
            }
            supabase_insert("presets", preset_row)
            presets_created += 1

    return {
        "ok": True,
        "toolsets_imported": toolsets_imported,
        "tools_imported": tools_imported,
        "presets_created": presets_created,
        "warnings": warnings,
    }
