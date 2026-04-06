"""
data_fetcher_openrefine.py
--------------------------
Alternate version of data_fetcher.py for use when loading data into OpenRefine.

The only difference from data_fetcher.py: after each file is downloaded,
all literal '#' characters in the file are replaced with '[hashmark]'.
This is a workaround for OpenRefine's known issue with '#' in data files.

Output is saved to a separate subfolder (e.g., reports/TDT/items_openrefine/)
so modified files are never confused with the originals.

Usage examples (same as data_fetcher.py):
    python data_fetcher_openrefine.py wx_tdt
    python data_fetcher_openrefine.py wx_tdt --stats
    python data_fetcher_openrefine.py wx_tdt --patrons
    python data_fetcher_openrefine.py wx_tdt --recent 3
    python data_fetcher_openrefine.py wx_tdt --since 2025-01-01
    python data_fetcher_openrefine.py --print-fingerprint
"""

import os
import re
import argparse
import paramiko
from dotenv import load_dotenv
from datetime import datetime, date
from sftp_utils import get_credentials, connect_sftp, list_remote_files, print_server_fingerprint


# ---------------------------------------------------------------------------
# Environment setup
# ---------------------------------------------------------------------------

load_dotenv()

EXPECTED_FINGERPRINT = os.getenv("FINGERPRINT")
HOST = os.getenv("HOST_NAME")
PORT = int(os.getenv("HOST_PORT", "22"))

DISCOVERY = False  # Toggle to True to discover fingerprint, or use --print-fingerprint


# ---------------------------------------------------------------------------
# Hashmark replacement (the key difference from data_fetcher.py)
# ---------------------------------------------------------------------------

def replace_hashmarks(file_path: str) -> int:
    """
    Replace all '#' characters with '[hashmark]' in a text file.

    Reads the file, performs the replacement in memory, then writes
    the modified content back to the same file path.

    Args:
        file_path: Path to the local file to modify

    Returns:
        Number of '#' characters that were replaced

    Example:
        count = replace_hashmarks("reports/TDT/items_openrefine/TDT.report.txt")
        # File is updated in place; '#' -> '[hashmark]' throughout
    """
    with open(file_path, "r", encoding="utf-8", errors="replace") as f:
        content = f.read()

    count = content.count("#")

    if count > 0:
        content = content.replace("#", "[hashmark]")
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"  Replaced {count} '#' character(s) with '[hashmark]'")
    else:
        print("  No '#' characters found in file")

    return count


# ---------------------------------------------------------------------------
# Argument parsing (identical to data_fetcher.py)
# ---------------------------------------------------------------------------

def _parse_since(s: str) -> date:
    s = s.strip()
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    raise argparse.ArgumentTypeError("Use YYYY-MM-DD")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Download reports from OCLC SFTP (OpenRefine version: replaces '#' with '[hashmark]')."
    )
    parser.add_argument(
        "lib_code",
        nargs="?",
        help="Credential key (e.g., wx_tdt). Required to find <LIB_CODE>_USER/<LIB_CODE>_PASS env vars."
    )
    parser.add_argument(
        "--recent",
        nargs="?",
        const=1,
        type=int,
        help="Download only the N most recent files (default 1 if flag present without a number)."
    )
    parser.add_argument(
        "--since",
        type=_parse_since,
        help="Only download files on/after this date (YYYY-MM-DD or YYYYMMDD)."
    )
    parser.add_argument(
        "--print-fingerprint",
        action="store_true",
        help="Print server SSH fingerprint and exit (no credentials needed)."
    )
    grp = parser.add_mutually_exclusive_group()
    grp.add_argument("--stats", action="store_true", help="Download WorldShare update report files")
    grp.add_argument("--patrons", action="store_true", help="Download Circulation Patron report files")
    return parser.parse_args()


# ---------------------------------------------------------------------------
# File filtering helpers (identical to data_fetcher.py)
# ---------------------------------------------------------------------------

def _filter_names(all_files: list[str], symbol: str, kind: str) -> list[str]:
    symbol = symbol.upper()

    def is_item(fn: str) -> bool:
        return (
            fn.startswith(f"{symbol}.")
            and "Circulation_Item_Inventories" in fn
            and fn.endswith(".txt")
        )

    def is_stat(fn: str) -> bool:
        return (
            (fn.startswith(f"{symbol}.") or fn.startswith(f"{symbol}D") or fn.startswith(f"{symbol}.D"))
            and (".report." in fn or ".exception" in fn or "Report_wk" in fn)
            and fn.endswith(".txt")
        )

    def is_patron(fn: str) -> bool:
        return (
            fn.startswith(f"{symbol}.")
            and "Circulation_Patron_Report_Full" in fn
            and (fn.endswith(".txt") or fn.endswith(".csv"))
        )

    pred = is_item if kind == "items" else (is_stat if kind == "stats" else is_patron)
    return [f for f in all_files if pred(f)]


def _file_date(fn: str, kind: str) -> date | None:
    if kind == "items":
        m = re.search(r"\.(\d{8})\.txt$", fn)
        if m:
            return datetime.strptime(m.group(1), "%Y%m%d").date()

    if kind == "stats":
        m = list(re.finditer(r"(\d{4}-\d{2}-\d{2})", fn))
        if m:
            return datetime.strptime(m[-1].group(1), "%Y-%m-%d").date()
        # Accommodate additional report date format in list
        m = re.search(r"(\d{8})", fn)
        if m:
            return datetime.strptime(m.group(1), "%Y%m%d").date()

    if kind == "patrons":
        m = re.search(r"(\d{8})", fn)
        if m:
            return datetime.strptime(m.group(1), "%Y%m%d").date()

    return None


# ---------------------------------------------------------------------------
# Main download function
# ---------------------------------------------------------------------------

def download_reports(lib_code: str, recent_count: int | None, want_stats: bool, want_patrons: bool, since: date | None):
    user, pwd = get_credentials(lib_code)

    # Determine which kind to fetch (default = items)
    kind = "items"
    if want_stats:
        kind = "stats"
    elif want_patrons:
        kind = "patrons"

    # Extract symbol from lib_code (e.g., 'wx_tdt' -> 'TDT')
    try:
        symbol = lib_code.split("_", 1)[1].upper()
    except IndexError:
        symbol = lib_code.upper()

    # Save to a separate '_openrefine' subfolder to avoid mixing with originals
    base_dir = os.path.join("reports", symbol)
    type_subdir = (
        "items_openrefine" if kind == "items"
        else ("stats_openrefine" if kind == "stats"
              else "patrons_openrefine")
    )
    local_dir = os.path.join(base_dir, type_subdir)
    os.makedirs(local_dir, exist_ok=True)

    print(f"\nOpenRefine mode: files will be saved to: {local_dir}")
    print("All '#' characters will be replaced with '[hashmark]'\n")

    # Connect to SFTP
    ssh, sftp = connect_sftp(user, pwd, verify=True)

    try:
        sftp.chdir("/xfer/wms/reports")
        all_files = list_remote_files(sftp, "/xfer/wms/reports")

        filtered = _filter_names(all_files, symbol, kind)

        if since is not None:
            kept = []
            for fn in filtered:
                d = _file_date(fn, kind)
                if d is None or d >= since:
                    kept.append(fn)
            filtered = kept

        filtered.sort(reverse=True)  # newest first

        if recent_count is not None and recent_count > 0:
            filtered = filtered[:recent_count]

        for filename in sorted(filtered):  # oldest first for readable progress
            local_path = os.path.join(local_dir, filename)
            if not os.path.exists(local_path):
                print(f"Downloading {filename}")
                sftp.get(filename, local_path)

                # Replace '#' with '[hashmark]' immediately after download
                replace_hashmarks(local_path)
            else:
                print(f"Already exists (skipping): {filename}")

        sftp.close()
        ssh.close()

    except paramiko.SSHException as e:
        if DISCOVERY and "Discovery complete" in str(e):
            print("\nDiscovery mode completed successfully!")
        else:
            print("SSH connection failed:", e)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    args = parse_args()

    if args.print_fingerprint:
        print_server_fingerprint()

    else:
        # Normal mode - require credentials from lib_code
        if not args.lib_code:
            print("Error: lib_code is required when not using --print-fingerprint")
            print("Example: python data_fetcher_openrefine.py wx_tdt")
            exit(1)

        download_reports(args.lib_code, args.recent, args.stats, args.patrons, args.since)
