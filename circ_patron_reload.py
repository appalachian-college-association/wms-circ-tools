#!/usr/bin/env python3
"""
Patron reload file builder (with optional barcode updates)

Make sure _USER and _PASS are added to .env, then include positional argument in the run:
example: python circ_patron_reload.py WX_ACACL --offline --use-expiration-date

Adds:
  - Trims whitespace on incoming Patron_Barcode and on barcode_updates.txt
    before matching and writing.
  - Preflight validator on "new" barcodes from barcode_updates.txt:
      * non-empty
      * <= --soft-max-barcode-len characters (default 20; configurable)
      * <= --hard-max-barcode-bytes bytes (default 30; configurable)
      * no duplicates inside the change list (warn)
      * warn if a "new" barcode already exists elsewhere in the incoming file
      * warn if reserved URL-encoding characters appear
  - Optional --sync-illid-to-barcode to copy updated barcodes into illId (Tipasa
    libraries need illId == barcode).
  - Optional --use-expiration-date to apply EXPIRATION_DATE from .env to oclcExpirationDate

See OCLC docs (patron tab-delimited and WMS barcode restrictions) for details.
"""

import os
import re
import sys
import csv
import argparse
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
import paramiko
from dotenv import load_dotenv

from sftp_utils import get_credentials, connect_sftp, list_remote_files, download_file
from file_utils import safe_read_txt
from data_loader import upload_patron_reload
from patron_validation import load_patron_updates, preflight_validate_updates
from patron_filtering import filter_patrons_by_criteria, write_skipped_patrons_report
from patron_formatting import FormatOptions, build_formatted_df, apply_patron_updates_if_any

# Set up module-level logger (configured in main())
logger = logging.getLogger(__name__)

# -----------------------------
# SFTP + general helpers
# -----------------------------

load_dotenv()

EXPECTED_FINGERPRINT = os.getenv("FINGERPRINT", "")
HOST = os.getenv("HOST_NAME", "")
PORT = int(os.getenv("HOST_PORT", "22"))


def _symbol_from_lib_code(lib_code: str) -> str:
    """Extract the library symbol from a credential key (e.g. 'wx_acacl' -> 'ACACL')."""
    parts = lib_code.split('_')
    if len(parts) >= 2:
        return parts[-1].upper()
    return lib_code.upper()


def download_patron_file_any_extension(
    sftp: paramiko.SFTPClient,
    remote_dir: str,
    base_pattern: str,
    downloads_dir: Path,
) -> Tuple[Path, str]:
    """
    Try to download patron file with either .txt or .csv extension.
    Returns the downloaded file path and the symbol extracted from filename.

    Args:
        sftp: SFTP client
        remote_dir: Remote directory path
        base_pattern: Base regex pattern (will be modified to try different extensions)
        downloads_dir: Local download directory
    """
    # Get list of files
    files = list_remote_files(sftp, remote_dir)

    # Try .txt first, then .csv
    extensions_to_try = ['.txt', '.csv']

    for ext in extensions_to_try:
        logger.info("Looking for files with %s extension...", ext)

        # Modify pattern to use current extension
        if base_pattern.endswith(r'\.txt\$'):
            current_pattern = base_pattern.replace(r'\.txt\$', f'\\{ext}$')
        else:
            # Assume the pattern ends with some extension, replace it
            current_pattern = re.sub(r'\\\.[a-zA-Z]+\$$', f'\\{ext}$', base_pattern)

        logger.info("Using pattern: %s", current_pattern)
        pattern = re.compile(current_pattern)

        try:
            latest_name, symbol = pick_latest_full_patron(files, pattern)
            logger.info("Found matching file: %s", latest_name)

            # Try to download
            txt_local = download_file(sftp, remote_dir, latest_name, downloads_dir)

            # Verify the file has content
            if txt_local.stat().st_size > 0:
                logger.info("Successfully downloaded %s with %s extension", latest_name, ext)
                return txt_local, symbol
            logger.warning("File %s was downloaded but is empty", latest_name)
        except FileNotFoundError as e:
            logger.warning("No matching %s files found: %s", ext, e)
            continue
        except (OSError, ValueError, paramiko.SSHException) as e:
            logger.warning("Failed to download %s file: %s", ext, e)
            continue

    # If we get here, neither extension worked
    raise FileNotFoundError("Could not download patron file with any extension (.txt, .csv)")


def get_flexible_pattern():
    """Return a pattern that can be easily modified for different extensions."""
    return r"^([A-Z]{3})\.Circulation_Patron_Report_Full\.(\d{8})\.(txt|csv)$"


def pick_latest_full_patron(files, pattern: re.Pattern) -> Tuple[str, str]:
    """
    Among files, pick the latest matching "<SYM>.Circulation_Patron_Report_Full.YYYYMMDD.txt"
    Returns (filename, symbol)
    """
    candidates = []
    for f in files:
        m = pattern.match(f)
        if m:
            sym = m.group(1)
            try:
                dt = datetime.strptime(m.group(2), "%Y%m%d")
            except ValueError:
                continue
            candidates.append((f, dt, sym))
    if not candidates:
        raise FileNotFoundError(
            "No matching full patron files found with the expected naming pattern."
        )
    f, _, sym = max(candidates, key=lambda x: x[1])
    return f, sym


def load_headers(headers_file: Path) -> list:
    """Read the tab-delimited headers file and return exactly 46 column names."""
    txt = headers_file.read_text(encoding="utf-8").strip()
    headers = [h.strip() for h in txt.split("\t") if h.strip()]
    if len(headers) != 46:
        raise ValueError(f"Expected 46 headers, found {len(headers)} in {headers_file}")
    return headers


def get_institution_id(lib_code: str) -> Optional[str]:
    """
    Get institution ID from environment variable.
    Expects format: {SYMBOL}_INSTITUTION_ID where SYMBOL is extracted from lib_code
    Example: For lib_code 'wx_acacl', looks for ACACL_INSTITUTION_ID=12345

    Args:
        lib_code: Library code (e.g., 'wx_acacl')

    Returns:
        Institution ID string, or None if not found
    """
    # Extract symbol from lib_code (e.g., 'ACACL' from 'wx_acacl')
    symbol = _symbol_from_lib_code(lib_code)

    env_var = f"{symbol}_INSTITUTION_ID"
    institution_id = os.getenv(env_var, "").strip()

    if not institution_id:
        logger.warning(
            "Institution ID not found for %s. Set %s in .env file.",
            lib_code, env_var
        )
        return None

    logger.info("Loaded institution ID from %s", env_var)
    return institution_id


def detect_symbol_from_txt(txt_path: Path) -> Optional[str]:
    """Sniff the inst_symbol column from the first rows of a patron file, if present."""
    try:
        df = pd.read_csv(txt_path, dtype=str, nrows=50)
    except (OSError, ValueError, pd.errors.ParserError, pd.errors.EmptyDataError):
        return None
    for col in df.columns:
        if col.lower() == "inst_symbol":
            vals = df[col].dropna().astype(str).str.strip()
            if not vals.empty:
                return vals.mode().iat[0] if not vals.mode().empty else vals.iloc[0]
    return None


# -----------------------------
# main() orchestration helpers
# -----------------------------

def build_arg_parser() -> argparse.ArgumentParser:
    """Build the command-line argument parser for the patron reload builder."""
    p = argparse.ArgumentParser(
        description="Build OCLC-formatted patron reload file, with optional patron "
                    "updates (barcode, email, etc.)."
    )
    p.add_argument(
        "lib_code",
        help="Credential key (e.g., wx_acacl). Used to find "
             "<LIB_CODE>_USER/<LIB_CODE>_PASS env vars.",
    )
    p.add_argument(
        "--offline", action="store_true",
        help="Skip SFTP download and use existing file in patronloads/downloads",
    )
    p.add_argument("--remote-dir", default="/xfer/wms/reports", help="Remote reports directory")
    p.add_argument(
        "--upload", action="store_true",
        help="If set, upload the result to /xfer/wms/in/patron",
    )
    p.add_argument(
        "--upload-test", action="store_true",
        help="If set, upload to TEST directory /xfer/wms/test/in/patron",
    )
    p.add_argument("--output-dir", default="patrons", help="Local base output directory")
    p.add_argument(
        "--headers-file", default="headers_formattedpatron.txt",
        help="Path to headers (46 columns)",
    )
    p.add_argument(
        "--project-root", default=".",
        help="Project root where patron_updates.txt (or barcode_updates.txt) may exist",
    )
    p.add_argument(
        "--can-self-edit", default="false", choices=["true", "false"],
        help="Default value for canSelfEdit",
    )
    p.add_argument(
        "--pattern",
        default=r"^([A-Z]{3})\.Circulation_Patron_Report_Full\.(\d{8})\.txt$",
        help="Regex to match patron full files",
    )
    p.add_argument(
        "--soft-max-barcode-len", type=int,
        default=int(os.getenv("SOFT_MAX_BARCODE_LEN", "20")),
        help="Soft character limit for 'new' barcodes (warn if exceeded). Default 20.",
    )
    p.add_argument(
        "--hard-max-barcode-bytes", type=int,
        default=int(os.getenv("HARD_MAX_BARCODE_BYTES", "30")),
        help="Hard byte-size guidance for WMS barcodes (warn if exceeded). Default 30.",
    )
    p.add_argument(
        "--sync-illid-to-barcode", action="store_true",
        help="If set, copy updated barcodes into illId for matched rows.",
    )
    p.add_argument(
        "--use-expiration-date", action="store_true",
        help="If set, apply EXPIRATION_DATE from .env to oclcExpirationDate field. "
             "Either do not enable or enter 'IGNORE' in .env to skip.",
    )
    p.add_argument(
        "--use-source-value", action="store_true",
        help="Extract FIRST part of pipe-delimited source fields (IdM values)",
    )
    p.add_argument(
        "--filter-email-domain", type=str, action="append",
        help="Only reload patrons with email containing this domain (e.g., @bethanywv.edu). "
             "Can be specified multiple times for multiple domains.",
    )
    p.add_argument(
        "--set-idsource-from-email", action="store_true",
        help="Use the matched email address as idAtSource (lowercase). "
             "Requires --filter-email-domain.",
    )
    p.add_argument(
        "--source-system", type=str,
        help="Set sourceSystem to this value for all reloaded patrons "
             "(e.g., https://sts.windows.net/...)",
    )
    return p


def _setup_logging(symbol: str) -> None:
    """Configure logging to both a per-library log file and stdout."""
    date_str = datetime.today().strftime("%m%d%y")
    log_name = f"{symbol}patronreload_{date_str}.log"
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    log_path = log_dir / log_name

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_path),
            logging.StreamHandler(sys.stdout)
        ]
    )


def _find_offline_file(args: argparse.Namespace, downloads_dir: Path) -> Tuple[Path, str]:
    """Locate the most recent existing patron full file for this library (offline mode)."""
    logger.info("Offline mode: looking for existing file in downloads directory")

    if not downloads_dir.exists():
        raise FileNotFoundError("Downloads directory does not exist. Run without --offline first.")

    # Determine the expected symbol from lib_code (e.g. 'wx_acacl' -> 'ACACL')
    parts = args.lib_code.split('_')
    if len(parts) < 2:
        raise ValueError(f"lib_code '{args.lib_code}' should contain underscore (e.g., wx_acacl)")
    expected_symbol = parts[-1].upper()

    logger.info("Looking for files matching symbol: %s", expected_symbol)

    # Find the most recent patron file for THIS library
    patron_files = []
    for ext in ['.txt', '.csv']:
        # Pattern now specifically matches the expected symbol
        pattern = re.compile(
            rf"^{expected_symbol}\.Circulation_Patron_Report_Full\.(\d{{8}})\{ext}$"
        )
        for file_path in downloads_dir.glob(
            f"{expected_symbol}.Circulation_Patron_Report_Full.*{ext}"
        ):
            m = pattern.match(file_path.name)
            if m:
                try:
                    file_date = datetime.strptime(m.group(1), "%Y%m%d")
                    patron_files.append((file_path, file_date, expected_symbol))
                except ValueError:
                    continue

    if not patron_files:
        raise FileNotFoundError(
            f"No patron files found for {expected_symbol} in downloads directory"
        )

    # Get the most recent file for this library
    txt_local, _, symbol = max(patron_files, key=lambda x: x[1])
    logger.info("Using existing file: %s", txt_local)
    return txt_local, symbol


def _download_via_sftp(args: argparse.Namespace, downloads_dir: Path) -> Tuple[Path, str]:
    """Connect to SFTP and download the latest patron full file, trying both extensions."""
    user, pwd = get_credentials(args.lib_code)
    ssh, sftp = connect_sftp(user, pwd, verify=True)
    try:
        try:
            # First try the original pattern (likely .txt)
            files = list_remote_files(sftp, args.remote_dir)
            pattern = re.compile(args.pattern)
            latest_name, symbol = pick_latest_full_patron(files, pattern)
            txt_local = download_file(sftp, args.remote_dir, latest_name, downloads_dir)
        except (FileNotFoundError, ValueError) as e:
            logger.warning("Failed with original pattern: %s", e)
            logger.info("Trying both .txt and .csv extensions...")
            # Try both extensions
            txt_local, symbol = download_patron_file_any_extension(
                sftp, args.remote_dir, get_flexible_pattern(), downloads_dir
            )
    finally:
        sftp.close()
    ssh.close()
    return txt_local, symbol


def _resolve_input_file(args: argparse.Namespace, output_dir: Path) -> Tuple[Path, str]:
    """Return (local patron file, symbol) from either offline lookup or SFTP download."""
    downloads_dir = output_dir / "downloads"
    if args.offline:
        return _find_offline_file(args, downloads_dir)
    return _download_via_sftp(args, downloads_dir)


def _read_incoming(txt_local: Path) -> pd.DataFrame:
    """Read the incoming patron file and trim whitespace on Patron_Barcode."""
    in_df = safe_read_txt(txt_local)

    # Trim whitespace on incoming Patron_Barcode to avoid phantom mismatches
    if "Patron_Barcode" in in_df.columns:
        before = in_df["Patron_Barcode"].astype(str)
        after = before.str.strip()
        trimmed = (before != after).sum()
        if trimmed:
            logger.info("Trimmed whitespace on %d incoming Patron_Barcode values", trimmed)
        in_df["Patron_Barcode"] = after
    else:
        logger.warning(
            "Incoming file does not include 'Patron_Barcode' column; "
            "cannot apply barcode updates."
        )
    return in_df


def _apply_email_filtering(
    in_df: pd.DataFrame, args: argparse.Namespace, output_dir: Path, symbol: str
) -> pd.DataFrame:
    """Apply --filter-email-domain filtering (if requested) and write a skipped report."""
    if not args.filter_email_domain:
        return in_df

    logger.info("Email domain filtering enabled")

    # Validate that domains start with @
    valid_domains = []
    for domain in args.filter_email_domain:
        if not domain.startswith('@'):
            domain = '@' + domain
        valid_domains.append(domain)

    # Filter patrons
    in_df, skipped_df = filter_patrons_by_criteria(in_df, valid_domains=valid_domains)

    # matched_email column already added by filter function
    if args.set_idsource_from_email:
        logger.info("Will use matched email as idAtSource")

    # Add source_system_value column if specified
    if args.source_system:
        logger.info("Will set sourceSystem to: %s", args.source_system)
        in_df['source_system_value'] = args.source_system

    # Write skipped patrons report
    if skipped_df is not None and not skipped_df.empty:
        write_skipped_patrons_report(skipped_df, output_dir, symbol)

    return in_df


def _load_and_validate_updates(
    args: argparse.Namespace, in_df: pd.DataFrame
) -> Optional[pd.DataFrame]:
    """Load patron_updates.txt (if present) and preflight-validate any new barcodes."""
    updates_df = load_patron_updates(Path(args.project_root))

    if updates_df is not None:
        # Only validate barcodes if patron_barcode_new column exists
        if "patron_barcode_new" in updates_df.columns:
            existing = set(
                in_df.get("Patron_Barcode", pd.Series([], dtype=str)).astype(str).str.strip()
            )
            updates_df = preflight_validate_updates(
                updates_df,
                existing_barcodes_in_file=existing,
                soft_max_chars=args.soft_max_barcode_len,
                hard_max_bytes=args.hard_max_barcode_bytes,
            )
    else:
        logger.info("No patron_updates.txt or barcode_updates.txt found; will load all rows.")

    return updates_df


def _apply_updates(
    in_df: pd.DataFrame, updates_df: Optional[pd.DataFrame], args: argparse.Namespace
) -> pd.DataFrame:
    """Apply patron updates, exiting on a wrong-library update-file error."""
    try:
        return apply_patron_updates_if_any(
            in_df, updates_df, sync_illid=args.sync_illid_to_barcode
        )
    except ValueError as e:
        # Error - wrong library's update file
        logger.error(str(e))
        sys.exit(1)


def _require_institution_id(args: argparse.Namespace) -> str:
    """Return the institution ID for this library, or exit if it is not configured."""
    institution_id = get_institution_id(args.lib_code)
    if institution_id is None:
        symbol = _symbol_from_lib_code(args.lib_code)
        logger.error(
            "Institution ID is REQUIRED for patron reload operations. "
            "Set %s_INSTITUTION_ID in .env file (e.g., %s_INSTITUTION_ID=12345)",
            symbol, symbol
        )
        sys.exit(1)
    return institution_id


def _write_output(
    in_df_updated: pd.DataFrame,
    args: argparse.Namespace,
    institution_id: str,
    symbol: str,
) -> Path:
    """Build the formatted 46-column reload file and write it to disk."""
    headers = load_headers(Path(args.headers_file))
    options = FormatOptions(
        can_self_edit=(args.can_self_edit == "true"),
        use_expiration_date=args.use_expiration_date,
        use_source_value=args.use_source_value,
    )
    formatted_df = build_formatted_df(in_df_updated, headers, institution_id, options)

    processed_dir = Path(args.output_dir) / "reloads"
    processed_dir.mkdir(parents=True, exist_ok=True)
    out_path = processed_dir / f"{symbol}patronreload.txt"
    formatted_df.to_csv(out_path, sep="\t", index=False, quoting=csv.QUOTE_MINIMAL)

    logger.info("Wrote reload file: %s (rows: %d)", out_path, len(formatted_df))
    return out_path


def _post_write_warnings(args: argparse.Namespace) -> None:
    """Remind the user to verify source fields when --use-source-value was used."""
    if args.use_source_value:
        logger.warning("\n%s", "=" * 60)
        logger.warning("IMPORTANT: VERIFY YOUR OUTPUT BEFORE UPLOADING")
        logger.warning("Check these fields in your output file:")
        logger.warning("  - idAtSource: Verify IDs from are current/correct")
        logger.warning("  - sourceSystem: Verify system identifier is correct")
        logger.warning("%s\n", "=" * 60)


def _upload_result(args: argparse.Namespace, out_path: Path) -> None:
    """Upload the reload file to the TEST or PRODUCTION directory if requested."""
    if not (args.upload or args.upload_test):
        return

    if args.upload_test:
        logger.info("Uploading reload file to TEST directory...")
        logger.warning("=" * 60)
        logger.warning("UPLOADING TO TEST ENVIRONMENT")
        logger.warning("Path: /xfer/wms/test/in/patron")
        logger.warning("=" * 60)
        remote_path = upload_patron_reload(
            args.lib_code, out_path, remote_dir="/xfer/wms/test/in/patron"
        )
    else:
        logger.info("Uploading reload file to PRODUCTION...")
        logger.warning("=" * 60)
        logger.warning("UPLOADING TO PRODUCTION ENVIRONMENT")
        logger.warning("Path: /xfer/wms/in/patron")
        logger.warning("=" * 60)
        remote_path = upload_patron_reload(args.lib_code, out_path)

    logger.info("Uploaded successfully: %s", remote_path)


def main(argv=None):
    """Entry point: download/locate the patron file, apply updates, and write the reload file."""
    args = build_arg_parser().parse_args(argv)
    symbol = _symbol_from_lib_code(args.lib_code)

    _setup_logging(symbol)

    logger.info("=" * 60)
    logger.info("Starting patron reload process")
    logger.info("=" * 60)
    logger.info("Library code: %s", args.lib_code)
    logger.info("Symbol: %s", symbol)

    # Resolve paths and locate the input file
    output_dir = Path(args.output_dir)
    txt_local, symbol = _resolve_input_file(args, output_dir)

    # Load, optionally filter, and update the incoming data
    in_df = _read_incoming(txt_local)
    in_df = _apply_email_filtering(in_df, args, output_dir, symbol)
    updates_df = _load_and_validate_updates(args, in_df)
    in_df_updated = _apply_updates(in_df, updates_df, args)

    # Determine symbol if we didn't get one from filename
    if not symbol:
        symbol = detect_symbol_from_txt(txt_local) or "UNK"

    # Get institution ID (required) and write the reload file
    institution_id = _require_institution_id(args)
    logger.info("Using institution ID: %s", institution_id)

    out_path = _write_output(in_df_updated, args, institution_id, symbol)
    _post_write_warnings(args)
    _upload_result(args, out_path)


if __name__ == "__main__":
    main()
