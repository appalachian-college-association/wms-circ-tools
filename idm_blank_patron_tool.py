#!/usr/bin/env python3
"""
IDM Blank-Name Patron Lookup & Delete Tool
-------------------------------------------
Uses OCLC's WorldShare Identity Management (IDM) API -- a DIFFERENT API than
the SFTP-based tools in this repo -- to look up specific patron records by
barcode or email, flag ones with a BLANK name (the "ghost" records that
cause duplicate-barcode errors on reload), and optionally delete them.

WHY THIS EXISTS:
Records created directly in WMS Admin (not through a source-system data load)
sometimes end up with no name attached. These "blank" records don't appear
in your normal Circulation_Patron_Report_Full download, so the other tools
in this repo can't see them -- but they DO collide on barcode when you try
to reload patrons with source values, causing a duplicate-barcode error.
This tool lets you check specific barcodes/emails directly against WMS via
the API, so you can find and clean these out before a reload.

HOW THIS IS DIFFERENT FROM THE OTHER SCRIPTS (read this if you're new to it):
  - The other scripts in this repo talk to OCLC over SFTP (a username and
    password, like logging into an FTP site).
  - This script talks to OCLC over a web API using OAuth2, which needs a
    DIFFERENT kind of credential called a "WSKey" (a Client ID + Client
    Secret pair). This is issued by OCLC specifically for API access and is
    NOT the same as your SFTP _USER/_PASS values. You'll need to request
    one from Beth (or OCLC support) if you don't already have one, with the
    Identity Management API's "SCIM" scopes enabled.
  - The API can only look up ONE specific barcode or email at a time -- it
    has no "give me every blank record at my library" option. That's why
    this tool works from a list you provide (see --review below), rather
    than scanning everything automatically.

TWO-STEP SAFETY WORKFLOW (same review-then-confirm pattern as
delete_expired_patrons.py elsewhere in this repo):

  STEP 1 -- REVIEW (safe, read-only, checks records but changes nothing):
      python idm_blank_patron_tool.py wx_wvb --review check_list.txt

    "check_list.txt" is a plain text file YOU create, with one barcode or
    email per line, e.g.:
        270236
        acbethany
        knedrow@bethanywv.edu

    This writes a CSV report to patrons/idm_review/ showing what it found
    for each one, including a "blank_name" Yes/No column. Nothing is
    deleted in this step.

  STEP 2 -- DELETE (destructive, requires your manual sign-off twice):
      python idm_blank_patron_tool.py wx_wvb --delete patrons/idm_review/WVB_idm_review_[...].csv

    Open the review CSV from Step 1 in Excel. For each row where
    blank_name = Yes that you've confirmed you want removed, type YES
    (all capitals) into the confirm_delete column. Save the file, then run
    this step. The script will ONLY delete rows where BOTH
    blank_name = Yes AND confirm_delete = YES, and it will show you the
    exact list and ask you to type "yes" one more time before doing
    anything irreversible.

.env FILE REQUIREMENTS (add these -- separate from your SFTP credentials):
    WVB_IDM_CLIENT_ID=your-wskey-client-id
    WVB_IDM_CLIENT_SECRET=your-wskey-client-secret
  (Reuses your existing WVB_INSTITUTION_ID value -- the IDM API calls this
  the "registry ID" and it's the same number.)

A NOTE ON TESTING THIS SCRIPT:
This was written directly from OCLC's published openapi_wms_IDM.yaml, but
that file doesn't fully spell out the exact shape of the OAuth token
request (it only gives the token URL and scope names). This script uses
the standard OAuth2 "client credentials" pattern with HTTP Basic
authentication, which is the most common approach for this kind of API --
but it hasn't been tested against a live WSKey yet. If get_access_token()
below fails with a 401 error on your first run, that's the first thing to
check with OCLC/Beth: whether they expect the client ID/secret sent as a
Basic auth header (what this script does) or as fields in the request body
instead. Everything else (the search and delete calls) follows the spec
exactly, so those should be more reliable once the token step works.
"""

import os
import csv
import time
import base64
import re
import logging
import argparse
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger("idm_blank_patron_tool")

TOKEN_URL = "https://oauth.oclc.org/token"

# The API allows 120 requests/minute (2/sec) per WSKey. We pause between
# calls to stay safely under that -- 0.6 seconds gives some buffer.
RATE_LIMIT_DELAY = 0.6

REVIEW_FIELDNAMES = [
    "searched_value", "search_type", "match_found", "principal_id",
    "institution_id", "given_name", "family_name", "oclc_username",
    "source_system", "id_at_source", "created", "last_modified",
    "blank_name", "confirm_delete", "notes",
]

# Matches a principal ID / PPID, e.g. 960b0082-f927-4ce8-89e1-e16867b4a4b1
# PPID is searchable directly in WMS Admin with User ID at Source index
# (the PPID you can copy straight out of a WMS Admin record's URL).
_UUID_PATTERN = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)

def get_idm_credentials(lib_code: str) -> tuple[str, str, str]:
    """
    Get IDM API credentials + institution/registry ID from .env.

    Looks for three values, built from the library's symbol (the part of
    lib_code after the underscore, uppercased -- e.g. 'wx_wvb' -> 'WVB'):
        <SYMBOL>_IDM_CLIENT_ID
        <SYMBOL>_IDM_CLIENT_SECRET
        <SYMBOL>_INSTITUTION_ID   (reused from your existing .env setup)
    """
    parts = lib_code.split("_")
    if len(parts) < 2:
        raise ValueError(f"lib_code '{lib_code}' should contain underscore (e.g., wx_wvb)")
    symbol = parts[-1].upper()

    client_id = os.getenv(f"{symbol}_IDM_CLIENT_ID")
    client_secret = os.getenv(f"{symbol}_IDM_CLIENT_SECRET")
    institution_id = os.getenv(f"{symbol}_INSTITUTION_ID")

    missing = [name for name, val in [
        (f"{symbol}_IDM_CLIENT_ID", client_id),
        (f"{symbol}_IDM_CLIENT_SECRET", client_secret),
        (f"{symbol}_INSTITUTION_ID", institution_id),
    ] if not val]

    if missing:
        raise ValueError(
            f"Missing required .env values for {lib_code}: {', '.join(missing)}"
        )

    return client_id, client_secret, institution_id


def get_access_token(client_id: str, client_secret: str, scope: str) -> str:
    """
    Exchange a Client ID + Client Secret (WSKey) for a short-lived access token.

    This is the OAuth2 "client credentials" flow: the SCRIPT is
    authenticating itself (not a specific person), which is the standard
    approach for an automated tool like this. The token comes back valid
    for a limited time and gets attached to every API call afterward.
    """
    credentials = f"{client_id}:{client_secret}"
    encoded = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")

    headers = {
        "Authorization": f"Basic {encoded}",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {
        "grant_type": "client_credentials",
        "scope": scope,
    }

    logger.info("Requesting access token from OCLC...")
    resp = requests.post(TOKEN_URL, headers=headers, data=data, timeout=30)

    if resp.status_code != 200:
        raise RuntimeError(
            f"Failed to get access token (status {resp.status_code}): {resp.text}\n"
            "Double check the WSKey Client ID/Secret in .env, and confirm "
            "with OCLC/Beth that this WSKey has SCIM scopes enabled."
        )

    token = resp.json().get("access_token")
    if not token:
        raise RuntimeError(f"Token response did not include access_token: {resp.text}")

    logger.info("Access token received")
    return token


def get_user_by_id(access_token: str, registry_id: str, principal_id: str) -> dict:
    """
    Look up a single user directly by principal ID (GET /Users/{id}).

    This bypasses search entirely, which matters for records that search
    can't find at all -- e.g. a record identified only by its oclcUserName
    (login name). That field isn't one of the two documented search
    filters (External_ID, EMAIL_ADDRESS), so records with no barcode, no
    correlation ID, and no email set are invisible to /Users/.search --
    but if you can navigate to the record in WMS Admin, the principal ID
    is sitting right in the browser's URL bar and this function can use
    it directly.
    """
    url = f"https://{registry_id}.share.worldcat.org/idaas/scim/v2/Users/{principal_id}"
    headers = {"Authorization": f"Bearer {access_token}"}

    resp = requests.get(url, headers=headers, timeout=30)
    time.sleep(RATE_LIMIT_DELAY)

    if resp.status_code != 200:
        logger.warning(
            "Direct lookup failed for principal_id '%s' (status %s): %s",
            principal_id, resp.status_code, resp.text
            )
        return {
            "match_found": False, "search_type": "principal_id (direct lookup)", "error": resp.text
            }

    return {
        "match_found": True,
        "search_type": "principal_id (direct lookup)",
        "multiple": False,
        "user": resp.json(),
    }


def search_user(access_token: str, registry_id: str, value: str) -> dict:
    """
    Look up a single user, either directly by principal ID (if the value
    looks like a UID) or by trying every filter type the API supports
    (External_ID, then EMAIL_ADDRESS) until one finds a match.

    The API really only has two distinct filter attributes, even though
    OCLC's docs describe three "kinds" of search:
      - External_ID  -- matches EITHER a barcode OR a correlation/source
                         system ID (idAtSource), depending on how the
                         institution's WSKey is configured. OCLC's own
                         examples use the identical "External_ID eq"
                         filter for both "search by barcode" and "search
                         by correlation info" -- they are NOT two separate
                         filters, just two kinds of values that can live
                         in that same field.
      - EMAIL_ADDRESS -- matches the SCIM emails[] value on the record.

    IMPORTANT: we do NOT guess which filter to use based on whether the
    value contains '@'. A correlation ID can itself look like an email
    address (OCLC's own spec example shows idAtSource: 'smithk@oclc.org'),
    so a value with '@' in it might still only be found via External_ID,
    not EMAIL_ADDRESS. Instead we just try External_ID first, then
    EMAIL_ADDRESS, and stop at whichever one finds a match.

    Returns a dict describing what happened -- always has "match_found"
    (True/False) and "search_type" (which filter succeeded, or a note
    that both were tried with no match); if a match was found, also has
    "user" (the raw record) and "multiple" (True if more than one record
    matched, which needs a human to look at it).
    """
    if _UUID_PATTERN.match(value.strip()):
        return get_user_by_id(access_token, registry_id, value.strip())

    search_attempts = [
        ("barcode_or_correlation_id", f'External_ID eq "{value}"'),
        ("email", f'EMAIL_ADDRESS eq "{value}"'),
    ]

    url = f"https://{registry_id}.share.worldcat.org/idaas/scim/v2/Users/.search"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/scim+json",
    }

    last_error = None
    for search_type, filter_str in search_attempts:
        body = {
            "schemas": ["urn:ietf:params:scim:api:messages:2.0:SearchRequest"],
            "filter": filter_str,
        }
        resp = requests.post(url, headers=headers, json=body, timeout=30)
        # Pace ourselves after EVERY actual API call, since one value can
        # now trigger up to two calls (External_ID, then EMAIL_ADDRESS).
        time.sleep(RATE_LIMIT_DELAY)

        if resp.status_code != 200:
            logger.warning(
                "Search failed for '%s' as %s (status %s): %s",
                value, search_type, resp.status_code, resp.text
                )
            last_error = resp.text
            continue

        result = resp.json()
        total = result.get("totalResults", 0)
        resources = result.get("Resources", [])

        if total == 0 or not resources:
            continue  # try the next filter type

        if total > 1:
            logger.warning(
                "'%s' matched %s records as %s -- needs manual review",
                value, total, search_type
                )
            return {
                "match_found": True,
                "search_type": search_type,
                "multiple": True,
                "user": resources[0]
                }

        return {
            "match_found": True,
                "search_type": search_type,
                "multiple": False,
                "user": resources[0]
                }

    # Neither filter found anything.
    result = {"match_found": False, "search_type": "external_id + email (no match)"}
    if last_error:
        result["error"] = last_error
    return result


def extract_user_fields(user: dict) -> dict:
    """Pull the fields we care about out of a raw SCIM user record."""
    name = user.get("name", {}) or {}
    given = (name.get("givenName") or "").strip()
    family = (name.get("familyName") or "").strip()

    persona = user.get("urn:mace:oclc.org:eidm:schema:persona:persona:20180305", {}) or {}
    correlation = user.get(
        "urn:mace:oclc.org:eidm:schema:persona:correlationinfo:20180101", {}
        ) or {}
    corr_list = correlation.get("correlationInfo", []) or []
    source_system = corr_list[0].get("sourceSystem", "") if corr_list else ""
    id_at_source = corr_list[0].get("idAtSource", "") if corr_list else ""

    meta = user.get("meta", {}) or {}

    return {
        "principal_id": user.get("id", ""),
        "institution_id": persona.get("institutionId", ""),
        "given_name": given,
        "family_name": family,
        "oclc_username": persona.get("oclcUsername", ""),
        "source_system": source_system,
        "id_at_source": id_at_source,
        "created": meta.get("created", ""),
        "last_modified": meta.get("lastModified", ""),
        "blank_name": "Yes" if (given == "" and family == "") else "No",
    }


def _write_csv(path: Path, fieldnames: list, rows: list) -> None:
    """Write a list of dict rows to a CSV file with a header."""
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _review_one(token: str, registry_id: str, value: str, index: int, total: int) -> dict:
    """Search for one value, log progress, and build its review-CSV row."""
    logger.info("[%s/%s] Checking: %s", index, total, value)
    result = search_user(token, registry_id, value)

    row = {fn: "" for fn in REVIEW_FIELDNAMES}
    row["searched_value"] = value
    row["search_type"] = result.get("search_type", "")
    row["match_found"] = "Yes" if result.get("match_found") else "No"

    if result.get("match_found"):
        row.update(extract_user_fields(result["user"]))
        if result.get("multiple"):
            row["notes"] = "MULTIPLE MATCHES FOUND - showing first only, review manually"
    elif "error" in result:
        row["notes"] = f"Search error: {result['error'][:200]}"

    time.sleep(RATE_LIMIT_DELAY)
    return row


def run_review(lib_code: str, input_file: Path, output_dir: Path) -> Path:
    """Step 1: check a list of barcodes/emails and write a review CSV. Read-only."""
    client_id, client_secret, registry_id = get_idm_credentials(lib_code)
    token = get_access_token(client_id, client_secret, scope="SCIM:read_user")

    values = [
        line.strip() for line in input_file.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    logger.info("Checking %s barcodes/emails against WMS...", len(values))

    output_dir.mkdir(parents=True, exist_ok=True)
    symbol = lib_code.split("_")[-1].upper()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = output_dir / f"{symbol}_idm_review_{timestamp}.csv"

    rows = [
        _review_one(token, registry_id, value, i, len(values))
        for i, value in enumerate(values, start=1)
    ]
    _write_csv(out_path, REVIEW_FIELDNAMES, rows)

    blank_count = sum(1 for r in rows if r["blank_name"] == "Yes")
    logger.info(
        "Review complete: %s checked, %s blank-name matches found",
        len(rows), blank_count
        )
    logger.info("Review file written to: %s", out_path)
    logger.info(
        "Next step: open this file, type YES in the confirm_delete column for "
        "records you want removed, save, then run with --delete"
    )
    return out_path


def _load_delete_candidates(review_file: Path) -> list:
    """Read a reviewed CSV and return only the rows explicitly confirmed for deletion."""
    with open(review_file, "r", newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    return [
        r for r in rows
        if r.get("blank_name", "").strip().lower() == "yes"
        and r.get("confirm_delete", "").strip().upper() == "YES"
        and r.get("principal_id", "").strip()
    ]


def _confirm_deletion(candidates: list, review_file: Path) -> bool:
    """Print what will be deleted and get explicit manual sign-off."""
    print(f"\n{'='*60}")
    print("DELETE CONFIRMATION REQUIRED")
    print(f"{'='*60}")
    print(f"Review file: {review_file}")
    print(f"Records marked for deletion: {len(candidates)}\n")
    for r in candidates[:15]:
        print(f"  - {r['searched_value']}  (principal_id: {r['principal_id']})")
    if len(candidates) > 15:
        print(f"  ... and {len(candidates) - 15} more")

    print("\nWARNING: This will PERMANENTLY DELETE these patron records from WMS.")
    print("This cannot be undone.")
    response = input("\nType 'yes' to proceed, anything else to cancel: ").strip().lower()
    return response == "yes"


def _delete_one(token: str, registry_id: str, candidate: dict, index: int, total: int) -> dict:
    """Delete a single confirmed record and return its delete-log row."""
    pid = candidate["principal_id"]
    value = candidate["searched_value"]
    logger.info("[%s/%s] Deleting: %s (principal_id: %s)", index, total, value, pid)

    url = f"https://{registry_id}.share.worldcat.org/idaas/scim/v2/Users/{pid}"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.delete(url, headers=headers, timeout=30)

    success = resp.status_code == 200
    logger.info(
        "  Deleted successfully" if success
        else f"  FAILED (status {resp.status_code}): {resp.text}")

    time.sleep(RATE_LIMIT_DELAY)

    return {
        "searched_value": value,
        "principal_id": pid,
        "status_code": resp.status_code,
        "success": success,
        "response": resp.text[:300],
    }


def run_delete(lib_code: str, review_file: Path) -> None:
    """Step 2: delete only the rows explicitly confirmed in a review CSV."""
    client_id, client_secret, registry_id = get_idm_credentials(lib_code)

    candidates = _load_delete_candidates(review_file)
    if not candidates:
        logger.info(
            "No rows are marked for deletion. A row needs blank_name=Yes AND "
            "confirm_delete=YES (all capitals) to be included."
        )
        return

    if not _confirm_deletion(candidates, review_file):
        logger.info("Delete cancelled.")
        return

    token = get_access_token(client_id, client_secret, scope="SCIM:delete_user")

    results = [
        _delete_one(token, registry_id, r, i, len(candidates))
        for i, r in enumerate(candidates, start=1)
    ]

    symbol = lib_code.split("_")[-1].upper()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = review_file.parent / f"{symbol}_idm_delete_log_{timestamp}.csv"
    _write_csv(
        log_path,
        ["searched_value", "principal_id", "status_code", "success", "response"],
        results,
        )

    succeeded = sum(1 for r in results if r["success"])
    logger.info("\nDone: %s/%s deleted successfully", succeeded, len(candidates))
    logger.info("Delete log written to: %s", log_path)


def main():
    """Parse arguments and dispatch to review or delete."""
    p = argparse.ArgumentParser(
        description="Look up blank-name WMS patron records via IDM API, and optionally delete them."
    )
    p.add_argument("lib_code", help="Credential key (e.g., wx_wvb)")
    p.add_argument(
        "--output-dir",
        default="patrons/idm_review",
        help="Where review/delete-log CSVs are written"
        )

    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--review",
        metavar="INPUT_FILE",
        help="Text file of barcodes/emails, one per line"
        )
    group.add_argument(
        "--delete",
        metavar="REVIEW_FILE",
        help="A reviewed CSV from a previous --review run")

    args = p.parse_args()
    output_dir = Path(args.output_dir)

    if args.review:
        run_review(args.lib_code, Path(args.review), output_dir)
    else:
        run_delete(args.lib_code, Path(args.delete))


if __name__ == "__main__":
    main()
