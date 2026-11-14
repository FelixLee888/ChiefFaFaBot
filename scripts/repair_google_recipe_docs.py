#!/usr/bin/env python3
"""Repair existing Chief Fafa Google Doc titles and add polished recipe content."""

from __future__ import annotations

import argparse
import concurrent.futures
import importlib.util
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

import requests


DOC_PREFIX = "Chief Fafa - "
CLEAN_MARKER = "Chef Fafa Clean Recipe v2"
PLACEHOLDER_RE = re.compile(r"not clearly detected|unavailable|missing|^n/?a$", re.IGNORECASE)


def load_pipeline(path: Path):
    spec = importlib.util.spec_from_file_location("chief_fafa_recipe_pipeline", path)
    if not spec or not spec.loader:
        raise RuntimeError(f"Unable to load pipeline: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def auth_headers(pipeline) -> Tuple[Dict[str, str], str]:
    token, err = pipeline.resolve_docs_access_token()
    if not token:
        raise RuntimeError(err or "Google Docs token unavailable")
    headers = {"Authorization": f"Bearer {token}"}
    quota_project = pipeline.resolve_docs_quota_project()
    if quota_project:
        headers["X-Goog-User-Project"] = quota_project
    return headers, quota_project


def list_recipe_docs(pipeline, headers: Dict[str, str]) -> List[Dict[str, Any]]:
    apostrophe = chr(39)
    query = (
        "mimeType="
        + apostrophe
        + "application/vnd.google-apps.document"
        + apostrophe
        + " and trashed=false"
    )
    files: List[Dict[str, Any]] = []
    page_token = ""
    while True:
        params = {
            "q": query,
            "spaces": "drive",
            "pageSize": 100,
            "orderBy": "createdTime desc",
            "fields": "nextPageToken,files(id,name,createdTime,modifiedTime,webViewLink)",
        }
        if page_token:
            params["pageToken"] = page_token
        response = requests.get(
            pipeline.GOOGLE_DRIVE_API_BASE,
            headers=headers,
            params=params,
            timeout=30,
        )
        data = response.json() if response.text else {}
        if response.status_code >= 400:
            raise RuntimeError(f"Drive list HTTP {response.status_code}: {data}")
        page_files = data.get("files", []) if isinstance(data, dict) else []
        files.extend(
            item
            for item in page_files
            if isinstance(item, dict) and str(item.get("name", "")).startswith(DOC_PREFIX)
        )
        page_token = str(data.get("nextPageToken", "")).strip() if isinstance(data, dict) else ""
        if not page_token:
            return files


def export_doc_text(pipeline, headers: Dict[str, str], doc_id: str) -> str:
    response = requests.get(
        f"{pipeline.GOOGLE_DRIVE_API_BASE}/{doc_id}/export",
        headers=headers,
        params={"mimeType": "text/plain"},
        timeout=30,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"Drive export HTTP {response.status_code}: {response.text[:300]}")
    return response.content.decode("utf-8-sig", errors="replace")


def extract_section(text: str, heading: str, next_headings: List[str]) -> str:
    start = text.find(heading)
    if start < 0:
        return ""
    start += len(heading)
    end = len(text)
    for next_heading in next_headings:
        pos = text.find(next_heading, start)
        if pos >= 0:
            end = min(end, pos)
    return text[start:end].strip()


def clean_lines(lines: List[str], max_items: int = 120) -> List[str]:
    result: List[str] = []
    seen = set()
    for raw in lines:
        item = re.sub(r"\s+", " ", str(raw or "")).strip(" -\t\r\n")
        item = re.sub(r"^\d+[.)]\s*", "", item).strip()
        if not item or PLACEHOLDER_RE.search(item):
            continue
        key = item.casefold()
        if key in seen:
            continue
        seen.add(key)
        result.append(item)
        if len(result) >= max_items:
            break
    return result


def parse_recipe_doc(item: Dict[str, Any], text: str) -> Dict[str, Any]:
    name = str(item.get("name", "")).strip()
    name_title = name[len(DOC_PREFIX) :].strip() if name.startswith(DOC_PREFIX) else name
    title_match = re.search(r"^Recipe Title:\s*(.+)$", text, flags=re.MULTILINE)
    title = title_match.group(1).strip() if title_match else name_title
    url_match = re.search(r"https?://[^\s]+", extract_section(text, "Original Page URL:", ["Source Type:", "Recipe Summary:"]))
    source_url = url_match.group(0).strip() if url_match else ""
    summary = extract_section(
        text,
        "Recipe Summary:",
        ["Food Image:", "Video Description (from original source):", "Ingredients (from original source):"],
    )
    description = extract_section(
        text,
        "Video Description (from original source):",
        ["Transcript Excerpt (auto):", "Ingredients (from original source):"],
    )
    transcript = extract_section(
        text,
        "Transcript Excerpt (auto):",
        ["Ingredients (from original source):"],
    )
    ingredient_block = extract_section(
        text,
        "Ingredients (from original source):",
        ["Method / Steps (from original source):"],
    )
    step_block = extract_section(text, "Method / Steps (from original source):", [])
    ingredients = clean_lines(ingredient_block.splitlines(), max_items=100)
    steps = clean_lines(step_block.splitlines(), max_items=120)
    return {
        "id": str(item.get("id", "")).strip(),
        "name": name,
        "title": title,
        "source_url": source_url,
        "summary": summary,
        "description": description,
        "transcript": transcript,
        "ingredients": ingredients,
        "steps": steps,
        "text": text,
        "webViewLink": str(item.get("webViewLink", "")).strip(),
    }


def polish_recipe(pipeline, record: Dict[str, Any]) -> Dict[str, Any]:
    evidence = {
        "current_title": record["title"],
        "source_url": record["source_url"],
        "current_summary": record["summary"][:1200],
        "source_description": record["description"][:6000],
        "transcript_excerpt": record["transcript"][:3500],
        "ingredients": record["ingredients"][:80],
        "steps": record["steps"][:80],
    }
    system_prompt = (
        "You are repairing an existing recipe record. Use only supplied evidence and preserve the dominant source "
        "language. Return strict JSON with keys title, summary, ingredients, steps, notes. The title must name the "
        "finished dish, not an ingredient, amount, account name, author, platform label, promotional phrase, or "
        "caption fragment. Keep a good existing dish title when it is already accurate. Summary must be one or two "
        "useful sentences. Deduplicate ingredients without changing quantities. Turn supported method details into "
        "clear ordered steps; never invent missing temperatures, times, or ingredients. Notes is an array of factual "
        "tips or storage guidance supported by the evidence."
    )
    parsed, err = pipeline.call_openai_responses_json(
        system_prompt,
        "Recipe record:\n" + json.dumps(evidence, ensure_ascii=False),
    )
    if err:
        return {"id": record["id"], "ok": False, "error": err}

    candidate_title = pipeline.cleanup_title_text(str(parsed.get("title", "")).strip())
    if not candidate_title or pipeline.title_looks_generic(candidate_title):
        return {"id": record["id"], "ok": False, "error": "model returned a weak title"}
    if len(candidate_title) > 80:
        candidate_title = pipeline.short_recipe_title(candidate_title)

    ingredients = clean_lines(pipeline.list_from_any(parsed.get("ingredients", []), max_items=120), max_items=100)
    steps = clean_lines(pipeline.list_from_any(parsed.get("steps", []), max_items=140), max_items=120)
    notes = clean_lines(pipeline.list_from_any(parsed.get("notes", []), max_items=30), max_items=20)
    summary = pipeline.normalize_space(str(parsed.get("summary", "")).strip())
    if not summary:
        summary = pipeline.normalize_space(record["summary"])
    return {
        "id": record["id"],
        "ok": True,
        "title": candidate_title,
        "summary": summary[:1200],
        "ingredients": ingredients or record["ingredients"],
        "steps": steps or record["steps"],
        "notes": notes,
    }


def format_clean_section(result: Dict[str, Any], source_url: str) -> str:
    lines = [result["title"], CLEAN_MARKER, "", "Summary", result["summary"] or "Summary unavailable.", ""]
    lines.append("Ingredients")
    if result["ingredients"]:
        lines.extend(f"- {item}" for item in result["ingredients"])
    else:
        lines.append("- Ingredients were not clearly available in the source.")
    lines.extend(["", "Method"])
    if result["steps"]:
        lines.extend(f"{index}. {step}" for index, step in enumerate(result["steps"], start=1))
    else:
        lines.append("1. Method details were not clearly available in the source.")
    if result["notes"]:
        lines.extend(["", "Notes"])
        lines.extend(f"- {note}" for note in result["notes"])
    if source_url:
        lines.extend(["", "Source", source_url])
    lines.extend(["", "--- Original Source Record ---", "", ""])
    return "\n".join(lines)


def utf16_len(text: str) -> int:
    return len(text.encode("utf-16-le")) // 2


def style_request(text: str, label: str, style: str) -> Dict[str, Any]:
    offset = text.find(label)
    start = 1 + utf16_len(text[:offset])
    end = start + utf16_len(label) + 1
    return {
        "updateParagraphStyle": {
            "range": {"startIndex": start, "endIndex": end},
            "paragraphStyle": {"namedStyleType": style},
            "fields": "namedStyleType",
        }
    }


def apply_repair(
    pipeline,
    headers: Dict[str, str],
    record: Dict[str, Any],
    result: Dict[str, Any],
) -> Tuple[bool, str]:
    doc_id = record["id"]
    section = format_clean_section(result, record["source_url"])
    requests_payload: List[Dict[str, Any]] = [{"insertText": {"location": {"index": 1}, "text": section}}]
    requests_payload.append(style_request(section, result["title"], "TITLE"))
    for heading in ("Summary", "Ingredients", "Method", "Notes", "Source", "--- Original Source Record ---"):
        if heading in section:
            requests_payload.append(style_request(section, heading, "HEADING_2"))
    old_title_line = f"Recipe Title: {record['title']}"
    new_title_line = f"Recipe Title: {result['title']}"
    if old_title_line != new_title_line:
        requests_payload.append(
            {
                "replaceAllText": {
                    "containsText": {"text": old_title_line, "matchCase": True},
                    "replaceText": new_title_line,
                }
            }
        )

    doc_headers = dict(headers)
    doc_headers["Content-Type"] = "application/json"
    response = requests.post(
        f"{pipeline.DOCS_API_CREATE_URL}/{doc_id}:batchUpdate",
        headers=doc_headers,
        json={"requests": requests_payload},
        timeout=45,
    )
    if response.status_code >= 400:
        return False, f"Docs update HTTP {response.status_code}: {response.text[:500]}"

    new_name = (DOC_PREFIX + result["title"])[:200]
    rename_response = requests.patch(
        f"{pipeline.GOOGLE_DRIVE_API_BASE}/{doc_id}",
        headers=doc_headers,
        params={"fields": "id,name"},
        json={"name": new_name},
        timeout=30,
    )
    if rename_response.status_code >= 400:
        return False, f"Drive rename HTTP {rename_response.status_code}: {rename_response.text[:500]}"
    return True, ""


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Apply changes; otherwise produce a dry-run report")
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--pipeline", default=str(Path(__file__).with_name("chief_fafa_recipe_pipeline.py")))
    parser.add_argument("--backup-dir", default="/home/felixlee/.openclaw/backups/2026-11-14-google-doc-repair")
    args = parser.parse_args()

    pipeline = load_pipeline(Path(args.pipeline))
    headers, _ = auth_headers(pipeline)
    files = list_recipe_docs(pipeline, headers)
    if args.limit > 0:
        files = files[: args.limit]

    backup_dir = Path(args.backup_dir)
    backup_dir.mkdir(parents=True, exist_ok=True)
    records: List[Dict[str, Any]] = []
    for item in files:
        try:
            text = export_doc_text(pipeline, headers, str(item.get("id", "")))
            record = parse_recipe_doc(item, text)
            records.append(record)
            (backup_dir / f"{record['id']}.txt").write_text(text, encoding="utf-8")
        except Exception as exc:
            print(json.dumps({"id": item.get("id"), "name": item.get("name"), "ok": False, "error": str(exc)}, ensure_ascii=False), flush=True)

    results_by_id: Dict[str, Dict[str, Any]] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, min(5, args.workers))) as pool:
        future_map = {pool.submit(polish_recipe, pipeline, record): record for record in records}
        for future in concurrent.futures.as_completed(future_map):
            record = future_map[future]
            try:
                result = future.result()
            except Exception as exc:
                result = {"id": record["id"], "ok": False, "error": str(exc)}
            results_by_id[record["id"]] = result
            if result.get("ok"):
                print(json.dumps({"id": record["id"], "old": record["title"], "new": result["title"], "ingredients": len(result["ingredients"]), "steps": len(result["steps"])}, ensure_ascii=False), flush=True)
            else:
                print(json.dumps({"id": record["id"], "old": record["title"], "ok": False, "error": result.get("error", "unknown")}, ensure_ascii=False), flush=True)

    report: List[Dict[str, Any]] = []
    for record in records:
        result = results_by_id.get(record["id"], {"ok": False, "error": "missing result"})
        entry = {
            "id": record["id"],
            "url": record["webViewLink"],
            "old_name": record["name"],
            "old_title": record["title"],
            "new_title": result.get("title", ""),
            "ok": bool(result.get("ok")),
            "error": result.get("error", ""),
            "applied": False,
        }
        if args.apply and result.get("ok"):
            if CLEAN_MARKER in record["text"]:
                entry["error"] = "clean section already present; skipped"
            else:
                applied, apply_error = apply_repair(pipeline, headers, record, result)
                entry["applied"] = applied
                entry["error"] = apply_error
        report.append(entry)

    report_path = backup_dir / ("apply-report.json" if args.apply else "dry-run-report.json")
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    summary = {
        "documents": len(records),
        "polished": sum(1 for x in report if x["ok"]),
        "applied": sum(1 for x in report if x["applied"]),
        "failed": sum(1 for x in report if not x["ok"] or (args.apply and x["ok"] and not x["applied"])),
        "report": str(report_path),
    }
    print(json.dumps(summary, ensure_ascii=False), flush=True)
    return 0 if summary["failed"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
