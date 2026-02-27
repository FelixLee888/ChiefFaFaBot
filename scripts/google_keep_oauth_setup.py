#!/usr/bin/env python3
"""Google OAuth helper for Chief Fafa note targets.

Step 1:
  python3 google_keep_oauth_setup.py --client-secret-file <path>
  -> open printed URL, approve, copy `code` from redirected URL.

Step 2:
  python3 google_keep_oauth_setup.py --client-secret-file <path> --code "<CODE>"
  -> prints env lines to store in .env for refresh-token flow.
"""

from __future__ import annotations

import argparse
import json
from urllib.parse import quote

import requests

AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
TOKEN_URL = "https://oauth2.googleapis.com/token"
DOCS_SCOPE = "https://www.googleapis.com/auth/documents"
KEEP_SCOPE = "https://www.googleapis.com/auth/keep"
DEFAULT_REDIRECT = "http://127.0.0.1:8788/callback"


def load_client(path: str) -> tuple[str, str]:
    raw = json.loads(open(path, "r", encoding="utf-8").read())
    block = raw.get("installed") if isinstance(raw.get("installed"), dict) else raw.get("web")
    if not isinstance(block, dict):
        raise ValueError("invalid OAuth client secret JSON")
    client_id = str(block.get("client_id", "")).strip()
    client_secret = str(block.get("client_secret", "")).strip()
    if not client_id or not client_secret:
        raise ValueError("client_id/client_secret missing in JSON")
    return client_id, client_secret


def build_auth_url(client_id: str, redirect_uri: str, scope: str, login_hint: str = "") -> str:
    params = {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": scope,
        "access_type": "offline",
        "prompt": "consent",
    }
    if login_hint.strip():
        params["login_hint"] = login_hint.strip()
    query = "&".join(f"{k}={quote(v, safe='')}" for k, v in params.items())
    return f"{AUTH_URL}?{query}"


def exchange_code(client_id: str, client_secret: str, redirect_uri: str, code: str) -> dict:
    payload = {
        "code": code,
        "client_id": client_id,
        "client_secret": client_secret,
        "redirect_uri": redirect_uri,
        "grant_type": "authorization_code",
    }
    resp = requests.post(TOKEN_URL, data=payload, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, dict):
        raise RuntimeError("token response is not JSON object")
    return data


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate OAuth refresh token for Google Docs or Keep.")
    parser.add_argument("--client-secret-file", required=True, help="OAuth client secret JSON file")
    parser.add_argument("--redirect-uri", default=DEFAULT_REDIRECT, help="OAuth redirect URI")
    parser.add_argument(
        "--scope",
        default=DOCS_SCOPE,
        help=f"OAuth scope (default: {DOCS_SCOPE})",
    )
    parser.add_argument("--login-hint", default="", help="Optional Google account email hint")
    parser.add_argument("--code", default="", help="Authorization code returned by Google")
    args = parser.parse_args()

    client_id, client_secret = load_client(args.client_secret_file)
    if not args.code:
        print("Open this URL in browser and authorize with the target Google account:")
        print(build_auth_url(client_id, args.redirect_uri, args.scope, args.login_hint))
        print("")
        print("After consent, copy the `code` query parameter from redirected URL and rerun with --code.")
        return

    data = exchange_code(client_id, client_secret, args.redirect_uri, args.code.strip())
    refresh_token = str(data.get("refresh_token", "")).strip()
    access_token = str(data.get("access_token", "")).strip()
    if not refresh_token:
        print("No refresh_token returned. Re-run auth with prompt=consent and a new code.")
        if access_token:
            print("Temporary access_token was returned, but it will expire.")
        return

    use_keep_env = args.scope.strip() == KEEP_SCOPE
    env_prefix = "GOOGLE_KEEP" if use_keep_env else "GOOGLE_DOCS"
    print("Set these values in /home/felixlee/Desktop/chief-fafa/.env:")
    print(f"{env_prefix}_CLIENT_ID={client_id}")
    print(f"{env_prefix}_CLIENT_SECRET={client_secret}")
    print(f"{env_prefix}_REFRESH_TOKEN={refresh_token}")
    if access_token:
        print(f"{env_prefix}_ACCESS_TOKEN={access_token}")


if __name__ == "__main__":
    main()
