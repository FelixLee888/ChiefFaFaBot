# TOOLS.md - Chief Fafa

## Core command
/usr/bin/python3 /home/felixlee/Desktop/chief-fafa/scripts/chief_fafa_recipe_pipeline.py --json --json-brief "<URL>"

## Output dir
/home/felixlee/Desktop/chief-fafa/notes

## Google Docs auth env vars
- GOOGLE_DOCS_ACCESS_TOKEN
- or GOOGLE_DOCS_REFRESH_TOKEN + GOOGLE_DOCS_CLIENT_ID + GOOGLE_DOCS_CLIENT_SECRET
- Optional: GOOGLE_DOCS_CLIENT_SECRET_FILE, GOOGLE_DOCS_QUOTA_PROJECT

## Backward compatibility
The script still accepts GOOGLE_KEEP_* as fallback env aliases.
