#!/usr/bin/env python3
"""
check_source.py
---------------
Find active patrons whose OCLC record lacks the campus IdM source values and
build a patron_updates.txt that adds them; list the rest for manual review.

PURPOSE
-------
Libraries whose patrons sign in through a campus identity provider (for WVB,
the Bethany College Azure AD tenant) need each OCLC record to carry a matching
pair of source values:

    Patron_User_ID_At_Source  = the patron's campus email  (e.g. jdoe@bethanywv.edu)
    Patron_Source_System      = the campus sourceSystem    (e.g. https://sts.windows.net/<tenant>/)

OCLC never deletes old source values, so these two fields are pipe-delimited
lists that also hold OCLC IDM GUIDs, "urn:mace:oclc:idm:..." entries, and
"barcode.update" junk. A record is fine as long as ONE campus pair exists at
the SAME position in both lists.

This script reproduces the OpenRefine history in
openrefine/WVBpatron_check_source.json (minus its delete-list cell.cross):

  1. Skip expired patrons and borrower categories outside the include list.
  2. Set aside accounts with WMS staff roles (User_Account_Roles) - review only.
  3. Skip records that already have a verified campus pair.
  4. For the rest, take the campus email from Patron_Email_Address, or from
     Patron_Username, and write a patron_updates.txt row:
         patron_barcode_old = patron_barcode_new = Patron_Barcode
         idAtSource         = campus email (lowercase)
         sourceSystem       = campus sourceSystem
  5. Records with no usable campus email (none, or only an excluded address
     such as library@<domain>), or sharing an email with another patron, go to
     a review file so the library can supply the correct address.

CONFIGURATION (per library symbol)
----------------------------------
In .env:
    WVB_CAMPUS_DOMAIN=bethanywv.edu
    WVB_CAMPUS_SOURCE_SYSTEM=https://sts.windows.net/e7f0b6cf-3723-46f1-a1ea-e81e6753374d/
or on the command line with --domain / --source-system (CLI wins).

USAGE
-----
    python data_fetcher.py wx_wvb --patrons --recent    # newest full patron report
    python check_source.py wx_wvb --dry-run             # preview counts
    python check_source.py wx_wvb                       # write patron_updates.txt + review file

    Optional flags:
      --domain             Campus email domain (default: <SYM>_CAMPUS_DOMAIN in .env)
      --source-system      Campus sourceSystem value (default: <SYM>_CAMPUS_SOURCE_SYSTEM)
      --exclude-email      Address that is never a valid idAtSource; repeatable
                           (default: library@<domain>)
      --include-category   Borrower category substring to include; repeatable
                           (default: "Faculty/Staff" and "Student")
      --patron-file        Full patron report to read (default: newest in
                           patrons/downloads/ or reports/<SYM>/patrons/)
      --output-file        Where to write patron_updates.txt (default: project root)
      --review-dir         Where to write <SYM>_source_review_YYYYMMDD.txt
                           (default: patrons/reports)
      --dry-run            Print what would be written; write nothing

AFTER RUNNING
-------------
    python circ_patron_reload.py wx_wvb --offline
    # review patrons/reloads/WVBpatronreload.txt, then
    python circ_patron_reload.py wx_wvb --upload-file patrons/reloads/WVBpatronreload.txt

The reload contains ONLY the patrons in patron_updates.txt (inner match on
patron_barcode_old), and takes idAtSource/sourceSystem from that file, so
--use-source-value is not needed. Remove patron_updates.txt when finished.
"""

import argparse
import csv
import logging
import os
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from dotenv import load_dotenv

from file_utils import safe_read_txt, find_latest_patron_report

load_dotenv()

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Column names in the OCLC full patron report
# ---------------------------------------------------------------------------

COL_BARCODE = "Patron_Barcode"
COL_EMAIL = "Patron_Email_Address"
COL_USERNAME = "Patron_Username"
COL_ID_AT_SOURCE = "Patron_User_ID_At_Source"
COL_SOURCE_SYSTEM = "Patron_Source_System"
COL_EXPIRATION = "Patron_Expiration_Date"
COL_CATEGORY = "Patron_Borrower_Category"
COL_ROLES = "User_Account_Roles"
COL_FAMILY = "Patron_Family_Name"
COL_GIVEN = "Patron_Given_Name"

REQUIRED_COLUMNS = [
    COL_BARCODE, COL_EMAIL, COL_USERNAME, COL_ID_AT_SOURCE,
    COL_SOURCE_SYSTEM, COL_EXPIRATION, COL_CATEGORY,
]

# ---------------------------------------------------------------------------
# Status values (one per patron), in the order the checks are applied
# ---------------------------------------------------------------------------

EXPIRED = "expired"
EXCLUDED_CATEGORY = "excluded_category"
WMS_ROLES = "wms_roles"
VERIFIED = "verified"
RELOAD_FROM_EMAIL = "reload_from_email"
RELOAD_FROM_USERNAME = "reload_from_username"
REVIEW_EXCLUDED_EMAIL = "review_excluded_email"
REVIEW_NO_CAMPUS_EMAIL = "review_no_campus_email"
REVIEW_SHARED_EMAIL = "review_shared_email"

STATUS_ORDER = [
    EXPIRED, EXCLUDED_CATEGORY, WMS_ROLES, VERIFIED,
    RELOAD_FROM_EMAIL, RELOAD_FROM_USERNAME,
    REVIEW_EXCLUDED_EMAIL, REVIEW_NO_CAMPUS_EMAIL, REVIEW_SHARED_EMAIL,
]
RELOAD_STATUSES = {RELOAD_FROM_EMAIL, RELOAD_FROM_USERNAME}
REVIEW_STATUSES = {WMS_ROLES, REVIEW_EXCLUDED_EMAIL, REVIEW_NO_CAMPUS_EMAIL, REVIEW_SHARED_EMAIL}

UPDATES_COLUMNS = ["patron_barcode_old", "patron_barcode_new", "idAtSource", "sourceSystem"]
REVIEW_COLUMNS = [
    "barcode", "familyName", "givenName", "borrowerCategory", "expirationDate",
    "email", "username", "current_idAtSource", "current_sourceSystem", "reason",
]


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class SourceConfig:
    """Per-library rules for what counts as a valid campus source pair."""
    domain: str
    source_system: str
    excluded_emails: list
    include_categories: list
    campus_re: re.Pattern = field(init=False, repr=False)

    def __post_init__(self):
        self.domain = self.domain.strip().lower().lstrip("@")
        self.source_system = self.source_system.strip()
        self.excluded_emails = [e.strip().lower() for e in self.excluded_emails if e.strip()]
        # Whole field must be exactly one address on the campus domain
        # (same rule as the OpenRefine recipe: /^[^ \@\,\;]+@bethanywv.edu$/)
        self.campus_re = re.compile(r"^[^\s@,;]+@" + re.escape(self.domain) + r"$", re.IGNORECASE)


def _derive_symbol(lib_code: str) -> str:
    """'wx_wvb' -> 'WVB'."""
    parts = lib_code.split("_")
    if len(parts) < 2:
        raise ValueError(f"lib_code '{lib_code}' should contain an underscore (e.g., wx_wvb)")
    return parts[-1].upper()


def build_config(args: argparse.Namespace, symbol: str) -> SourceConfig:
    """Resolve domain/sourceSystem from CLI flags, falling back to .env."""
    domain_var = f"{symbol}_CAMPUS_DOMAIN"
    system_var = f"{symbol}_CAMPUS_SOURCE_SYSTEM"

    domain = args.domain or os.getenv(domain_var, "")
    source_system = args.source_system or os.getenv(system_var, "")

    missing = [name for name, val in ((domain_var, domain), (system_var, source_system)) if not val]
    if missing:
        logger.error(
            "Campus source settings not found for %s. Add %s to .env "
            "(or pass --domain / --source-system).", symbol, " and ".join(missing)
        )
        sys.exit(1)

    domain = domain.strip().lower().lstrip("@")
    excluded = args.exclude_email if args.exclude_email else [f"library@{domain}"]
    categories = args.include_category if args.include_category else ["Faculty/Staff", "Student"]

    cfg = SourceConfig(domain, source_system, excluded, categories)
    logger.info("Campus domain: %s", cfg.domain)
    logger.info("Campus sourceSystem: %s", cfg.source_system)
    logger.info("Excluded idAtSource addresses: %s", ", ".join(cfg.excluded_emails) or "(none)")
    logger.info("Included borrower categories: %s", ", ".join(cfg.include_categories))
    return cfg


# ---------------------------------------------------------------------------
# Field checks
# ---------------------------------------------------------------------------

def campus_email(value, cfg: SourceConfig) -> tuple:
    """
    Return (email, excluded) for one field value.

    email    : the lowercased address if the whole field is one campus address
               and it is not on the exclude list; otherwise None
    excluded : True when the field IS a campus address but is excluded
               (e.g. library@bethanywv.edu)
    """
    text = "" if pd.isna(value) else str(value).strip()
    if not text or not cfg.campus_re.match(text):
        return None, False
    text = text.lower()
    if text in cfg.excluded_emails:
        return None, True
    return text, False


def _same_system(candidate: str, cfg: SourceConfig) -> bool:
    """Compare a sourceSystem entry to the configured one, ignoring a trailing slash."""
    return candidate.strip().rstrip("/") == cfg.source_system.rstrip("/")


def has_verified_pair(id_field, ss_field, cfg: SourceConfig) -> tuple:
    """
    Check the pipe-delimited idAtSource / sourceSystem lists for a campus pair.

    Returns (verified, misaligned):
      verified   : a campus email sits at the SAME position as the campus
                   sourceSystem (the strict rule). If the two lists have
                   different lengths we cannot pair positions, so we fall back
                   to "both lists contain a match" and report misaligned=True.
    """
    ids = [p.strip() for p in str(id_field).split("|")]
    systems = [p.strip() for p in str(ss_field).split("|")]

    if len(ids) == len(systems):
        for id_part, sys_part in zip(ids, systems):
            if campus_email(id_part, cfg)[0] and _same_system(sys_part, cfg):
                return True, False
        return False, False

    any_email = any(campus_email(p, cfg)[0] for p in ids)
    any_system = any(_same_system(p, cfg) for p in systems)
    return (any_email and any_system), True


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------

def _eligibility_status(row: pd.Series, cfg: SourceConfig, today: pd.Timestamp) -> Optional[str]:
    """Recipe steps 4-6: expired, excluded category, WMS roles. None = still eligible."""
    expires = pd.to_datetime(row.get(COL_EXPIRATION, ""), errors="coerce")
    if pd.notna(expires) and expires.normalize() < today:
        return EXPIRED

    category = str(row.get(COL_CATEGORY, ""))
    if not any(inc in category for inc in cfg.include_categories):
        return EXCLUDED_CATEGORY

    if str(row.get(COL_ROLES, "")).strip():
        return WMS_ROLES

    return None


def _source_status(row: pd.Series, cfg: SourceConfig) -> tuple:
    """
    Recipe steps 7-9 for an eligible row.
    Returns (status, chosen_email, reason, misaligned).
    """
    verified, misaligned = has_verified_pair(
        row.get(COL_ID_AT_SOURCE, ""), row.get(COL_SOURCE_SYSTEM, ""), cfg
    )
    if verified:
        return VERIFIED, "", "", misaligned

    email, email_excluded = campus_email(row.get(COL_EMAIL, ""), cfg)
    if email:
        return RELOAD_FROM_EMAIL, email, "", misaligned

    username, user_excluded = campus_email(row.get(COL_USERNAME, ""), cfg)
    if username:
        return RELOAD_FROM_USERNAME, username, "", misaligned

    if email_excluded or user_excluded:
        bad = str(row.get(COL_EMAIL, "")).strip() or str(row.get(COL_USERNAME, "")).strip()
        return REVIEW_EXCLUDED_EMAIL, "", f"only excluded email ({bad.lower()})", misaligned

    return REVIEW_NO_CAMPUS_EMAIL, "", f"no @{cfg.domain} email in email or username", misaligned


def _mark_shared_emails(df: pd.DataFrame) -> pd.DataFrame:
    """Reload candidates that share a chosen email cannot use it as a unique idAtSource."""
    reload_mask = df["status"].isin(RELOAD_STATUSES)
    counts = df.loc[reload_mask, "chosen_email"].value_counts()
    shared = counts[counts > 1]
    if shared.empty:
        return df

    logger.warning("%d campus email(s) are shared by more than one reload candidate:", len(shared))
    for email, n in shared.items():
        logger.warning("  %s: %d patrons", email, n)

    shared_mask = reload_mask & df["chosen_email"].isin(shared.index)
    df.loc[shared_mask, "reason"] = df.loc[shared_mask, "chosen_email"].map(
        lambda e: f"shared email {e} (used by {counts[e]} patrons)"
    )
    df.loc[shared_mask, "status"] = REVIEW_SHARED_EMAIL
    df.loc[shared_mask, "chosen_email"] = ""
    return df


def classify(df: pd.DataFrame, cfg: SourceConfig) -> pd.DataFrame:
    """Add status / chosen_email / reason columns to a copy of the patron report."""
    today = pd.Timestamp.today().normalize()
    statuses, emails, reasons = [], [], []
    misaligned_count = 0

    for _, row in df.iterrows():
        status = _eligibility_status(row, cfg, today)
        email, reason = "", ""
        if status == WMS_ROLES:
            reason = "has WMS roles - not reloaded"
        elif status is None:
            status, email, reason, misaligned = _source_status(row, cfg)
            misaligned_count += int(misaligned)
        statuses.append(status)
        emails.append(email)
        reasons.append(reason)

    out = df.copy()
    out["status"] = statuses
    out["chosen_email"] = emails
    out["reason"] = reasons

    if misaligned_count:
        logger.warning(
            "%d record(s) have different numbers of idAtSource and sourceSystem entries; "
            "for those the campus pair could not be checked by position "
            "(used 'both lists contain a match' instead).", misaligned_count
        )

    return _mark_shared_emails(out)


# ---------------------------------------------------------------------------
# Input / output
# ---------------------------------------------------------------------------

def _find_patron_report(symbol: str, patron_file: Optional[Path]) -> Path:
    """
    --patron-file if given; otherwise the newest Full report found in
    patrons/downloads/ or reports/<SYM>/patrons/ (same rule as
    circ_patron_reload.py --offline: newest date wins, patrons/downloads/ wins a tie).
    """
    if patron_file is not None:
        if not patron_file.exists():
            raise FileNotFoundError(f"--patron-file not found: {patron_file}")
        return patron_file
    search_dirs = [Path("patrons") / "downloads", Path("reports") / symbol / "patrons"]
    chosen, _ = find_latest_patron_report(symbol, search_dirs)
    return chosen


def load_patron_df(patron_path: Path) -> pd.DataFrame:
    """Read the full patron report and confirm the columns we need are present."""
    df = safe_read_txt(patron_path)
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(
            f"Patron report '{patron_path.name}' is missing required columns: "
            f"{', '.join(missing)}. Make sure this is a *Circulation_Patron_Report_Full* file."
        )
    if COL_ROLES not in df.columns:
        logger.warning("Column %s not found; WMS-role check skipped", COL_ROLES)
        df[COL_ROLES] = ""
    return df


def updates_rows(df: pd.DataFrame, cfg: SourceConfig) -> list:
    """Build patron_updates.txt rows from the reload candidates."""
    rows = []
    for _, row in df[df["status"].isin(RELOAD_STATUSES)].iterrows():
        barcode = str(row[COL_BARCODE]).strip()
        rows.append({
            "patron_barcode_old": barcode,
            "patron_barcode_new": barcode,
            "idAtSource": row["chosen_email"],
            "sourceSystem": cfg.source_system,
        })
    return rows


def review_rows(df: pd.DataFrame) -> list:
    """Build review-file rows for everything that needs a human."""
    rows = []
    for _, row in df[df["status"].isin(REVIEW_STATUSES)].iterrows():
        rows.append({
            "barcode": str(row[COL_BARCODE]).strip(),
            "familyName": row.get(COL_FAMILY, ""),
            "givenName": row.get(COL_GIVEN, ""),
            "borrowerCategory": row.get(COL_CATEGORY, ""),
            "expirationDate": str(row.get(COL_EXPIRATION, ""))[:10],
            "email": row.get(COL_EMAIL, ""),
            "username": row.get(COL_USERNAME, ""),
            "current_idAtSource": row.get(COL_ID_AT_SOURCE, ""),
            "current_sourceSystem": row.get(COL_SOURCE_SYSTEM, ""),
            "reason": row["reason"],
        })
    return rows


def _write_tsv(rows: list, fieldnames: list, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)
    logger.info("Wrote %d row(s) to %s", len(rows), path)


def _print_preview(title: str, rows: list, fieldnames: list, limit: int = 10) -> None:
    print(f"\n{title} ({len(rows)} rows; showing up to {limit}):")
    print("\t".join(fieldnames))
    for r in rows[:limit]:
        print("\t".join(str(r[f]) for f in fieldnames))


def _print_summary(df: pd.DataFrame, lib_code: str, updates_path: Path,
                   review_path: Optional[Path], dry_run: bool) -> None:
    counts = df["status"].value_counts()
    print()
    print("=" * 60)
    print("SUMMARY" + ("  (DRY RUN - nothing written)" if dry_run else ""))
    print("=" * 60)
    print(f"Patrons in report               : {len(df)}")
    for status in STATUS_ORDER:
        n = int(counts.get(status, 0))
        if n:
            print(f"  {status:<30}: {n}")
    n_reload = int(df["status"].isin(RELOAD_STATUSES).sum())
    n_review = int(df["status"].isin(REVIEW_STATUSES).sum())
    print(f"Reload rows -> {updates_path}: {n_reload}")
    if review_path is not None:
        print(f"Review rows -> {review_path}: {n_review}")
    else:
        print(f"Review rows: {n_review}")

    if n_reload and not dry_run:
        print()
        print("NEXT STEP:")
        print(f"  Review {updates_path}, then run:")
        print(f"    python circ_patron_reload.py {lib_code} --offline")
        print(f"  The reload file will contain ONLY the {n_reload} patron(s) in {updates_path}.")
        print(f"  Remove {updates_path} when done so it does not filter your next reload.")
    print("=" * 60)
    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Find patrons missing campus idAtSource/sourceSystem values, write "
            "patron_updates.txt for them, and a review file for the rest."
        )
    )
    p.add_argument(
        "lib_code", help="Library credential key, e.g. wx_wvb (symbol = part after '_')."
    )
    p.add_argument("--domain", help="Campus email domain (default: <SYM>_CAMPUS_DOMAIN in .env).")
    p.add_argument(
        "--source-system",
        help="Campus sourceSystem value (default: <SYM>_CAMPUS_SOURCE_SYSTEM in .env).",
    )
    p.add_argument(
        "--exclude-email", action="append", metavar="EMAIL",
        help="Address never valid as idAtSource; repeatable (default: library@<domain>).",
    )
    p.add_argument(
        "--include-category", action="append", metavar="TEXT",
        help="Borrower category substring to include; repeatable "
             "(default: 'Faculty/Staff' and 'Student').",
    )
    p.add_argument(
        "--patron-file", type=Path,
        help="Full patron report to read (default: newest in patrons/downloads/ "
             "or reports/<SYM>/patrons/).",
    )
    p.add_argument(
        "--output-file", type=Path, default=Path("patron_updates.txt"),
        help="Output path for patron_updates.txt (default: patron_updates.txt).",
    )
    p.add_argument(
        "--review-dir", type=Path, default=Path("patrons/reports"),
        help="Directory for <SYM>_source_review_YYYYMMDD.txt (default: patrons/reports).",
    )
    p.add_argument("--dry-run", action="store_true", help="Print results; write nothing.")
    return p


def main(argv=None) -> None:
    """Entry point."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s: %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    args = _build_arg_parser().parse_args(argv)

    symbol = _derive_symbol(args.lib_code)
    logger.info("Library symbol: %s", symbol)
    cfg = build_config(args, symbol)

    patron_path = _find_patron_report(symbol, args.patron_file)
    logger.info("Using patron report: %s", patron_path)
    df = classify(load_patron_df(patron_path), cfg)

    reload_rows = updates_rows(df, cfg)
    review = review_rows(df)
    review_path = args.review_dir / f"{symbol}_source_review_{datetime.today():%Y%m%d}.txt"

    if args.dry_run:
        _print_preview("patron_updates.txt rows", reload_rows, UPDATES_COLUMNS)
        _print_preview("review rows", review, REVIEW_COLUMNS)
        _print_summary(df, args.lib_code, args.output_file, review_path, dry_run=True)
        return

    if reload_rows:
        _write_tsv(reload_rows, UPDATES_COLUMNS, args.output_file)
    else:
        logger.info("No reload candidates; %s not written", args.output_file)
    if review:
        _write_tsv(review, REVIEW_COLUMNS, review_path)
    else:
        logger.info("Nothing to review; %s not written", review_path)
        review_path = None

    _print_summary(df, args.lib_code, args.output_file, review_path, dry_run=False)


if __name__ == "__main__":
    main()
