#!/usr/bin/env python3
"""Daily Google Docs OAuth health check for Chief Fafa.

The script is intentionally standalone so cron can run it without invoking the
LLM-facing recipe pipeline. It sends a Telegram notification only when the
OAuth refresh token cannot mint an access token and reauthorization is needed.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Dict, Iterable, Tuple

GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
DEFAULT_REDIRECT_URI = "http://127.0.0.1:8788/callback"
DEFAULT_LOGIN_HINT = "jancefelix@gmail.com"
DEFAULT_SCOPES = (
    "https://www.googleapis.com/auth/documents",
    "https://www.googleapis.com/auth/drive.file",
)
DEFAULT_CHAT_ID = "-5247351741"


def load_env_files(paths: Iterable[Path]) -> Dict[str, str]:
    values: Dict[str, str] = {}
    for path in paths:
        try:
            lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
        except OSError:
            continue
        for raw_line in lines:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, raw_value = line.split("=", 1)
            key = key.strip()
            value = raw_value.strip().strip("'\"")
            if key and value and key not in values:
                values[key] = value
    return values


def read_config() -> Dict[str, str]:
    env = load_env_files(
        [
            Path("/home/felixlee/Desktop/chief-fafa/.env"),
            Path("/home/felixlee/.openclaw/.env"),
            Path(__file__).resolve().parent.parent / ".env",
            Path.home() / ".openclaw/.env",
        ]
    )
    merged = dict(env)
    for key, value in os.environ.items():
        if value.strip():
            merged[key] = value.strip()
    return merged


def read_openclaw_chieffafa_token() -> str:
    for path in [
        Path("/home/felixlee/.openclaw/openclaw.json"),
        Path.home() / ".openclaw/openclaw.json",
    ]:
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        token = (
            data.get("channels", {})
            .get("telegram", {})
            .get("accounts", {})
            .get("chieffafa", {})
            .get("botToken", "")
        )
        if str(token).strip():
            return str(token).strip()
    return ""


def build_auth_url(config: Dict[str, str]) -> str:
    client_id = config.get("GOOGLE_DOCS_CLIENT_ID") or config.get("GOOGLE_KEEP_CLIENT_ID") or ""
    redirect_uri = config.get("GOOGLE_DOCS_REDIRECT_URI") or DEFAULT_REDIRECT_URI
    login_hint = config.get("GOOGLE_DOCS_LOGIN_HINT") or DEFAULT_LOGIN_HINT
    params = {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": " ".join(DEFAULT_SCOPES),
        "access_type": "offline",
        "prompt": "consent",
        "login_hint": login_hint,
    }
    return "https://accounts.google.com/o/oauth2/v2/auth?" + urllib.parse.urlencode(params)


def refresh_google_token(config: Dict[str, str]) -> Tuple[bool, str]:
    client_id = config.get("GOOGLE_DOCS_CLIENT_ID") or config.get("GOOGLE_KEEP_CLIENT_ID") or ""
    client_secret = config.get("GOOGLE_DOCS_CLIENT_SECRET") or config.get("GOOGLE_KEEP_CLIENT_SECRET") or ""
    refresh_token = config.get("GOOGLE_DOCS_REFRESH_TOKEN") or config.get("GOOGLE_KEEP_REFRESH_TOKEN") or ""
    if not client_id or not client_secret or not refresh_token:
        return False, "GOOGLE_DOCS_CLIENT_ID / GOOGLE_DOCS_CLIENT_SECRET / GOOGLE_DOCS_REFRESH_TOKEN missing"

    payload = urllib.parse.urlencode(
        {
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token",
        }
    ).encode("utf-8")
    request = urllib.request.Request(
        GOOGLE_TOKEN_URL,
        data=payload,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            body = json.loads(response.read().decode("utf-8"))
            return bool(body.get("access_token")), ""
    except urllib.error.HTTPError as exc:
        raw_body = exc.read().decode("utf-8", errors="ignore")
        try:
            body = json.loads(raw_body)
        except json.JSONDecodeError:
            body = {}
        detail = body.get("error_description") or body.get("error") or f"HTTP {exc.code}"
        return False, str(detail)
    except Exception as exc:
        return False, f"{exc.__class__.__name__}: {exc}"


def should_notify(error: str) -> bool:
    low = str(error or "").casefold()
    return (
        "invalid_grant" in low
        or "expired" in low
        or "revoked" in low
        or "missing" in low
    )


def send_telegram(config: Dict[str, str], text: str) -> None:
    token = (
        config.get("CHIEF_FAFA_TELEGRAM_BOT_TOKEN")
        or read_openclaw_chieffafa_token()
        or config.get("TELEGRAM_BOT_TOKEN")
        or ""
    )
    chat_id = config.get("CHIEF_FAFA_TELEGRAM_CHAT_ID") or DEFAULT_CHAT_ID
    if not token:
        raise RuntimeError("CHIEF_FAFA_TELEGRAM_BOT_TOKEN / TELEGRAM_BOT_TOKEN missing")

    payload = urllib.parse.urlencode(
        {
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": "true",
        }
    ).encode("utf-8")
    request = urllib.request.Request(
        f"https://api.telegram.org/bot{token}/sendMessage",
        data=payload,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        response.read()


def main() -> int:
    parser = argparse.ArgumentParser(description="Check Chief Fafa Google Docs OAuth token health.")
    parser.add_argument("--notify", action="store_true", help="Send Telegram message if reauthorization is needed.")
    parser.add_argument("--force-notify", action="store_true", help="Send Telegram message even when token is healthy.")
    parser.add_argument("--quiet", action="store_true", help="Reduce stdout output.")
    args = parser.parse_args()

    config = read_config()
    ok, error = refresh_google_token(config)
    auth_url = build_auth_url(config)
    status = {
        "ok": ok,
        "needs_reauth": not ok and should_notify(error),
        "error": error,
        "checked_at": int(time.time()),
    }
    if not args.quiet:
        print(json.dumps(status, ensure_ascii=True, indent=2))

    if args.notify and ((not ok and should_notify(error)) or args.force_notify):
        if ok:
            message = "Chef Fafa Google Docs OAuth daily check: token is healthy."
        else:
            message = (
                "Chef Fafa Google Docs OAuth daily check failed.\n"
                f"Error: {error}\n\n"
                "Reauthorize with the bot write-access URL:\n"
                f"{auth_url}\n\n"
                "After consent, reply to Chef Fafa Bot with the full callback URL."
            )
        send_telegram(config, message)

    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
