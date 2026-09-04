#!/usr/bin/env python3
"""
build_patron_updates.py
-----------------------
Parses an OCLC patron upload exception report and builds a patron_updates.txt
file for use with circ_patron_reload.py.

PURPOSE
-------
When OCLC patron loads fail with:

    Error occurred while creating a new user... COMPLETE_CREATE_FAILURE :
    new username is already used as a username by another user

...it means OCLC already has a record for that patron (matched by username/email)
but the incoming barcode does not match the barcode already on the OCLC record.
OCLC cannot overwrite the barcode unless the incoming record also supplies the
OCLC system-level match fields: idAtSource (the OCLC principalID/GUID) and
sourceSystem (the OCLC IDM URN for this library).

This script:
  1. Reads the exception report (.txt). Under each COMPLETE_CREATE_FAILURE
     line OCLC echoes the rejected record as a 46-column tab-delimited row
     (same layout as headers_formattedpatron.txt). We take the username and
     the INCOMING barcode from that row. By default the newest
     *.exception.*.txt for the symbol in reports/<SYMBOL>/stats/ is used.
  2. Looks up each username (Patron_Username, falling back to
     Patron_Email_Address) in the newest full patron report for the symbol
     (reports/<SYMBOL>/patrons/<SYMBOL>.Circulation_Patron_Report_Full.YYYYMMDD.txt,
     as downloaded by data_fetcher.py --patrons) to get the barcode currently
     on the OCLC record plus its IdM source fields.
  3. Cleans Patron_User_ID_At_Source / Patron_Source_System with the same
     first-part-of-pipe logic circ_patron_reload.py --use-source-value applies
     (patron_formatting.process_special_fields), so the values match what a
     reload would produce.
  4. Writes patron_updates.txt to the project root with the columns that
     circ_patron_reload.py expects:
         patron_barcode_old  patron_barcode_new  idAtSource  sourceSystem
     where patron_barcode_old = Patron_Barcode from the patron report (the
     barcode OCLC has now) and patron_barcode_new = the incoming barcode from
     the exception row (the barcode the load tried to set).

USAGE
-----
    python data_fetcher.py wx_twy --patrons --recent     # newest full patron report
    python data_fetcher.py wx_twy --stats --recent 2     # newest load report + exception
    python build_patron_updates.py wx_twy --dry-run      # preview
    python build_patron_updates.py wx_twy                # write patron_updates.txt

    Optional flags:
      --exception-file   Path to exception report (auto-detected if omitted)
      --patron-file      Path to a full patron report (auto-detected if omitted)
      --output-file      Output path (default: patron_updates.txt)
      --dry-run          Print results to console; do not write file

AFTER RUNNING
-------------
Review patron_updates.txt, then run the normal reload workflow:

    python circ_patron_reload.py wx_twy --offline --use-source-value

Because circ_patron_reload.py does an inner match on patron_barcode_old, the
resulting reload file contains ONLY the patrons listed in patron_updates.txt.
Remove or rename patron_updates.txt when you are done so it does not filter
your next reload.

TERMINOLOGY NOTES (for Python beginners)
-----------------------------------------
- "regex" / "pattern" : a text search rule. re.search() scans a string for
  a match. re.compile() pre-builds the pattern for speed when used many times.
- "DataFrame" (df): a pandas table of rows and columns, like a spreadsheet.
- "f-string": a string that starts with f"..." and lets you embed variable
  values directly inside curly braces, e.g.  f"Hello {name}".
- "argparse": the Python standard way to accept command-line flags like --dry-run.
- "glob": a wildcard file search, like *.txt in a folder.
"""

import argparse
import csv
import logging
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from dotenv import load_dotenv

from file_utils import safe_read_txt, find_latest_patron_report, load_headers
from patron_formatting import process_special_fields

load_dotenv()

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logger = logging.getLogger(__name__)

# Pattern that marks a failed-create line in the exception report.
# re.IGNORECASE makes the match case-insensitive, just in case.
_FAILURE_PATTERN = re.compile(
    r"COMPLETE_CREATE_FAILURE\s*:\s*new username is already used",
    re.IGNORECASE,
)

# OCLC stamps exception/report filenames with the run time, e.g.
#   TWY.D20260903.T0409.TWU_Patron_Upload.exception.2026-09-03_041951.txt
# We use that stamp (not the file's modified time) to decide which is newest.
_EXCEPTION_STAMP = re.compile(r"(\d{4}-\d{2}-\d{2})_(\d{6})\.txt$")

# Columns we need from the full patron report
_REQUIRED_PATRON_COLUMNS = {
    "Patron_Username",
    "Patron_Barcode",
    "Patron_User_ID_At_Source",
    "Patron_Source_System",
}

# The data line under each failure header is the rejected record echoed back
# in the 46-column tab-delimited reload layout (headers_formattedpatron.txt).
# We read the column positions from that file so 'barcode' and 'username'
# are found by name. If a data line does not have 46 tab-separated fields we
# fall back to taking the last whitespace token as the username.
_HEADERS_FILE = Path("headers_formattedpatron.txt")
_BARCODE_COL = "barcode"
_USERNAME_COL = "username"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _derive_symbol(lib_code: str) -> str:
    """
    Extract the three-letter library symbol from a lib_code like 'wx_twy'.

    >>> _derive_symbol('wx_twy')
    'TWY'
    """
    parts = lib_code.split("_")
    if len(parts) < 2:
        raise ValueError(
            f"lib_code '{lib_code}' should contain an underscore (e.g., wx_twy)"
        )
    return parts[-1].upper()


def _exception_sort_key(p: Path) -> datetime:
    """Newest-first key: the run stamp in the filename, else the file's modified time."""
    m = _EXCEPTION_STAMP.search(p.name)
    if m:
        try:
            return datetime.strptime(f"{m.group(1)}_{m.group(2)}", "%Y-%m-%d_%H%M%S")
        except ValueError:
            pass
    return datetime.fromtimestamp(p.stat().st_mtime)


def _find_exception_file(symbol: str) -> Path:
    """
    Auto-detect the most recent exception report for *symbol*.

    Search order: reports/<SYMBOL>/stats/ (where data_fetcher.py --stats saves
    them), then the current directory, then patrons/.

    OCLC names exception files like:
        TWY.D20260903.T0409.TWU_Patron_Upload.exception.2026-09-03_041951.txt

    We accept any .txt file whose name contains both the symbol and
    'exception' (case-insensitive) and pick the newest by the run stamp in
    the filename.
    """
    search_dirs = [Path("reports") / symbol / "stats", Path("."), Path("patrons")]

    candidates = []
    for d in search_dirs:
        if not d.exists():
            continue
        for p in d.glob("*.txt"):
            name_lower = p.name.lower()
            if symbol.lower() in name_lower and "exception" in name_lower:
                candidates.append(p)

    if not candidates:
        searched = ", ".join(str(d) for d in search_dirs)
        raise FileNotFoundError(
            f"No exception report found for symbol '{symbol}' in {searched}. "
            f"Run 'python data_fetcher.py wx_{symbol.lower()} --stats --recent' "
            "or use --exception-file."
        )

    logger.info("Found %d exception file(s); using the newest", len(candidates))
    return max(candidates, key=_exception_sort_key)


def _find_patron_report(symbol: str, patron_file: Optional[Path]) -> Path:
    """
    Return the full patron report to match against.

    Uses --patron-file if given; otherwise the newest
    <SYMBOL>.Circulation_Patron_Report_Full.YYYYMMDD.txt in reports/<SYMBOL>/patrons/.
    """
    if patron_file is not None:
        if not patron_file.exists():
            raise FileNotFoundError(f"--patron-file not found: {patron_file}")
        return patron_file

    chosen, _ = find_latest_patron_report(symbol, [Path("reports") / symbol / "patrons"])
    return chosen


# ---------------------------------------------------------------------------
# Core parsing
# ---------------------------------------------------------------------------

def _load_reload_column_index() -> Optional[dict]:
    """
    Map reload column name -> position (0-based) using headers_formattedpatron.txt.
    Returns None (with a warning) if the headers file is missing or malformed.
    """
    if not _HEADERS_FILE.exists():
        logger.warning(
            "%s not found; cannot read incoming barcodes from the exception rows",
            _HEADERS_FILE,
        )
        return None
    headers = load_headers(_HEADERS_FILE)
    index = {name: pos for pos, name in enumerate(headers)}
    if _BARCODE_COL not in index or _USERNAME_COL not in index:
        logger.warning(
            "%s does not contain '%s' and '%s' columns", _HEADERS_FILE, _BARCODE_COL, _USERNAME_COL
        )
        return None
    return index


def _parse_data_line(data_line: str, col_index: Optional[dict]) -> Optional[dict]:
    """
    Pull username and incoming barcode out of one echoed reload row.

    Returns {'username': ..., 'incoming_barcode': ...} or None if nothing usable.
    incoming_barcode is '' when the row could not be read positionally.
    """
    fields = data_line.split("\t")

    if col_index is not None and len(fields) == len(col_index):
        username = fields[col_index[_USERNAME_COL]].strip()
        barcode = fields[col_index[_BARCODE_COL]].strip()
        if username:
            return {"username": username, "incoming_barcode": barcode}

    # Fallback: last whitespace token is the username; barcode unknown
    tokens = [t for t in re.split(r"\s+", data_line.strip()) if t]
    if not tokens:
        return None
    logger.warning(
        "Data row did not have %s tab-delimited fields; using last token '%s' as "
        "username and leaving incoming barcode blank",
        len(col_index) if col_index else "the expected number of", tokens[-1],
    )
    return {"username": tokens[-1], "incoming_barcode": ""}


def _parse_failures(exception_path: Path) -> list:
    """
    Read the exception report and return one dict per COMPLETE_CREATE_FAILURE
    block: {'username': str, 'incoming_barcode': str}.

    HOW THE EXCEPTION FILE IS STRUCTURED
    Each failure looks like this (two lines):

        Error occurred while creating a new user... COMPLETE_CREATE_FAILURE :
        new username is already used as a username by another user
            Adrian  Hutchins  ...  2782  w115100  ...  ahutchins@tnwesleyan.edu

    The second line is the rejected record in the 46-column tab-delimited
    reload layout. 'barcode' (the INCOMING barcode the load tried to set) and
    'username' are read by position using headers_formattedpatron.txt.

    The list may contain duplicate usernames; duplicates are removed later.
    """
    col_index = _load_reload_column_index()
    failures = []
    lines = exception_path.read_text(encoding="utf-8", errors="replace").splitlines()

    i = 0
    while i < len(lines):
        line = lines[i]
        if _FAILURE_PATTERN.search(line):
            # The very next non-blank line is the patron data row
            j = i + 1
            while j < len(lines) and not lines[j].strip():
                j += 1
            if j < len(lines):
                parsed = _parse_data_line(lines[j], col_index)
                if parsed:
                    logger.info(
                        "Found failure: username=%s incoming_barcode=%s",
                        parsed["username"], parsed["incoming_barcode"] or "(unknown)",
                    )
                    failures.append(parsed)
                else:
                    logger.warning(
                        "COMPLETE_CREATE_FAILURE at line %d has no data line", i + 1
                    )
            i = j + 1
        else:
            i += 1

    return failures


def _load_patron_df(patron_path: Path) -> pd.DataFrame:
    """
    Read the full patron report (pipe-delimited; delimiter auto-detected) and
    clean the IdM source fields exactly the way circ_patron_reload.py does with
    --use-source-value.

    All values kept as strings to preserve leading zeros.
    """
    df = safe_read_txt(patron_path)
    logger.info(
        "Loaded patron report '%s': %d rows, %d columns",
        patron_path.name, len(df), len(df.columns),
    )
    _check_required_columns(df, patron_path)

    # Same cleaning circ_patron_reload.py applies: first part of pipe-delimited
    # idAtSource, and the most common sourceSystem value for every row.
    return process_special_fields(df)


def _check_required_columns(df: pd.DataFrame, patron_path: Path) -> None:
    """
    Verify the patron report has the columns we need.
    Raises ValueError with a clear message if any are missing.
    """
    missing = _REQUIRED_PATRON_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(
            f"Patron report '{patron_path.name}' is missing required columns: "
            f"{', '.join(sorted(missing))}\n"
            "Make sure this is a *Circulation_Patron_Report_Full* file, not a "
            "partial or weekly report."
        )


def _match_username(username: str, patron_df: pd.DataFrame) -> pd.DataFrame:
    """
    Return the patron rows whose Patron_Username equals *username*
    (case-insensitive). If none, fall back to Patron_Email_Address.
    """
    wanted = username.strip().lower()

    mask = patron_df["Patron_Username"].str.strip().str.lower() == wanted
    matches = patron_df[mask]
    if not matches.empty:
        return matches

    if "Patron_Email_Address" in patron_df.columns:
        mask = patron_df["Patron_Email_Address"].str.strip().str.lower() == wanted
        matches = patron_df[mask]
        if not matches.empty:
            logger.info(
                "No Patron_Username match for '%s'; matched on Patron_Email_Address instead",
                username,
            )
    return matches


def _build_updates_rows(failures: list, patron_df: pd.DataFrame) -> tuple:
    """
    For each failure, look the username up in the patron report and build an
    update row:
      patron_barcode_old = Patron_Barcode from the report (what OCLC has now)
      patron_barcode_new = incoming barcode from the exception row (what the
                           load tried to set); falls back to the old barcode
                           if the exception row could not be read.

    Returns (rows, not_found) where:
      - rows      : list of dicts ready to write to patron_updates.txt
      - not_found : list of usernames that had no match in the patron report
    """
    rows = []
    not_found = []
    seen = {}  # username -> incoming barcode already handled

    for failure in failures:
        username = failure["username"]
        incoming = failure["incoming_barcode"]

        if username in seen:
            if incoming and seen[username] and incoming != seen[username]:
                logger.warning(
                    "Username '%s' appears again with a different incoming barcode "
                    "(%s vs %s) - keeping the first", username, seen[username], incoming
                )
            continue
        seen[username] = incoming

        matches = _match_username(username, patron_df)

        if matches.empty:
            logger.warning("No match in patron report for username: %s", username)
            not_found.append(username)
            continue

        if len(matches) > 1:
            logger.warning(
                "Multiple rows match username '%s' - using first match only", username
            )

        row = matches.iloc[0]
        barcode = row.get("Patron_Barcode", "").strip()

        if not barcode:
            logger.warning(
                "Username '%s' matched but Patron_Barcode is empty - skipping", username
            )
            not_found.append(username)
            continue

        id_at_source = row.get("Patron_User_ID_At_Source", "").strip()
        source_system = row.get("Patron_Source_System", "").strip()
        if not id_at_source or not source_system:
            logger.warning(
                "Username '%s' has blank idAtSource/sourceSystem in the report - "
                "OCLC may still reject this row", username
            )

        new_barcode = incoming
        if not new_barcode:
            logger.warning(
                "Username '%s' has no incoming barcode in the exception row - "
                "keeping current barcode %s", username, barcode
            )
            new_barcode = barcode
        elif new_barcode == barcode:
            logger.info(
                "Username '%s': incoming barcode equals current barcode (%s); "
                "only source fields will change", username, barcode
            )

        rows.append({
            "patron_barcode_old": barcode,       # barcode on the OCLC record now
            "patron_barcode_new": new_barcode,   # barcode the failed load tried to set
            "idAtSource":   id_at_source,
            "sourceSystem": source_system,
        })

        logger.info(
            "Matched '%s' -> barcode %s -> %s idAtSource=%s sourceSystem=%s",
            username, barcode, new_barcode, id_at_source, source_system,
        )

    return rows, not_found


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def _write_updates_file(rows: list, output_path: Path) -> None:
    """
    Write patron_updates.txt as a tab-delimited file.

    The columns match exactly what circ_patron_reload.py's
    load_patron_updates() expects.
    """
    fieldnames = [
        "patron_barcode_old",
        "patron_barcode_new",
        "idAtSource",
        "sourceSystem",
    ]
    with output_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)

    logger.info("Wrote %d row(s) to %s", len(rows), output_path)


def _print_summary(rows: list, not_found: list, output_path: Path, lib_code: str) -> None:
    """Print a human-readable summary to the console."""
    print()
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Rows written to {output_path} : {len(rows)}")
    changed = sum(1 for r in rows if r["patron_barcode_old"] != r["patron_barcode_new"])
    print(f"Rows where the barcode will change : {changed}")

    if not_found:
        print()
        print(f"WARNING: {len(not_found)} username(s) had no match in the patron report:")
        for u in not_found:
            print(f"   - {u}")
        print()
        print("For unmatched patrons, check whether:")
        print("  1. The patron report is the most recent one")
        print(f"     (python data_fetcher.py {lib_code} --patrons --recent).")
        print("  2. The username in the exception report matches Patron_Username")
        print("     or Patron_Email_Address (including domain).")
        print("  3. The patron exists in WMS Circulation at all - the failure may")
        print("     be an IDM-only record (see idm_blank_patron_tool.py).")

    print()
    if rows:
        print("NEXT STEP:")
        print(f"  Review {output_path}, then run:")
        print(f"    python circ_patron_reload.py {lib_code} --offline --use-source-value")
        print(f"  The reload file will contain ONLY the patrons in {output_path}.")
    print("=" * 60)
    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _build_arg_parser() -> argparse.ArgumentParser:
    """Define CLI arguments."""
    p = argparse.ArgumentParser(
        description=(
            "Parse an OCLC patron upload exception report and build "
            "patron_updates.txt for use with circ_patron_reload.py."
        )
    )
    p.add_argument(
        "lib_code",
        help="Library credential key, e.g. wx_twy. Used to derive the three-letter symbol.",
    )
    p.add_argument(
        "--exception-file",
        type=Path,
        default=None,
        help=(
            "Path to the OCLC exception report (.txt). If omitted, the newest "
            "*exception*.txt for the symbol in reports/<SYMBOL>/stats/ (then ./ and "
            "patrons/) is used."
        ),
    )
    p.add_argument(
        "--patron-file",
        type=Path,
        default=None,
        help=(
            "Path to a full patron report to match against. If omitted, the newest "
            "<SYMBOL>.Circulation_Patron_Report_Full.YYYYMMDD.txt in "
            "reports/<SYMBOL>/patrons/ is used."
        ),
    )
    p.add_argument(
        "--output-file",
        type=Path,
        default=Path("patron_updates.txt"),
        help="Output path for patron_updates.txt "
             "(default: patron_updates.txt in current directory).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print results to console but do not write patron_updates.txt.",
    )
    return p


def main(argv=None) -> None:
    """Entry point."""
    # --- Set up logging ---
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s: %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    args = _build_arg_parser().parse_args(argv)

    # --- Derive symbol ---
    symbol = _derive_symbol(args.lib_code)
    logger.info("Library symbol: %s", symbol)

    # --- Locate exception file ---
    exception_path = args.exception_file
    if exception_path is None:
        logger.info("No --exception-file given; searching for exception report...")
        exception_path = _find_exception_file(symbol)
    logger.info("Using exception file: %s", exception_path)

    # --- Locate patron report ---
    patron_path = _find_patron_report(symbol, args.patron_file)
    logger.info("Using patron report: %s", patron_path)

    # --- Parse failures ---
    failures = _parse_failures(exception_path)
    if not failures:
        logger.error(
            "No COMPLETE_CREATE_FAILURE entries found in '%s'. "
            "Check that the file is the correct exception report.",
            exception_path,
        )
        sys.exit(1)

    unique_usernames = {f["username"] for f in failures}
    logger.info("Found %d unique failure username(s) to look up", len(unique_usernames))

    # --- Load patron report (validates columns, cleans source fields) ---
    patron_df = _load_patron_df(patron_path)

    # --- Build update rows ---
    rows, not_found = _build_updates_rows(failures, patron_df)

    if not rows:
        logger.error(
            "None of the failure usernames matched a patron in the patron report. "
            "Cannot build patron_updates.txt."
        )
        sys.exit(1)

    # --- Write or print ---
    if args.dry_run:
        print()
        print("DRY RUN - output not written. Rows that would be written:")
        print("\t".join(
            ["patron_barcode_old", "patron_barcode_new", "idAtSource", "sourceSystem"]
        ))
        for r in rows:
            print("\t".join([
                r["patron_barcode_old"], r["patron_barcode_new"],
                r["idAtSource"], r["sourceSystem"],
            ]))
    else:
        _write_updates_file(rows, args.output_file)

    _print_summary(rows, not_found, args.output_file, args.lib_code)


if __name__ == "__main__":
    main()
