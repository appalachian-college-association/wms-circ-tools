import os
import re
import base64
import hashlib
import argparse
import paramiko
from typing import Tuple
from dotenv import load_dotenv
from datetime import datetime, date
from sftp_utils import get_credentials, connect_sftp, list_remote_files, download_file, print_server_fingerprint


def parse_boolean(value, default=False):
    """
    Parse boolean values from environment variables.
    Accepts: true, yes, 1, on (case-insensitive) as True
    Everything else (including false, no, 0, off) as False
    """
    if value is None:
        return default
    # Convert to lowercase string for comparison
    str_value = str(value).lower().strip()
    
    # Values that mean "True"
    true_values = {'true', 'yes', '1', 'on'}
    
    return str_value in true_values

load_dotenv()

EXPECTED_FINGERPRINT = os.getenv("FINGERPRINT")
HOST = os.getenv("HOST_NAME")
PORT = int(os.getenv("HOST_PORT", "22"))

DISCOVERY = False  # Toggle to True to discover fingerprint or run with CLI flag --print_fingerprint

CREDENTIALS = {
    key: os.getenv(key) for key in os.environ if key.endswith("_USER") or key.endswith("_PASS")
}

def _parse_since(s: str) -> date:
    s = s.strip()
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    raise argparse.ArgumentTypeError("Use YYYY-MM-DD")

def parse_args():
    parser = argparse.ArgumentParser(description="Download reports from OCLC SFTP.")
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

def download_reports(lib_code: str, recent_count: int | None, want_stats: bool, want_patrons: bool, since: date | None):
    user, pwd = get_credentials(lib_code)

    # Determine which kind to fetch (default = items)
    kind = "items"
    if want_stats:
        kind = "stats"
    elif want_patrons:
        kind = "patrons"

    # compute symbol + destination folder reports/<SYMBOL>/<type>/
    try:
        symbol = lib_code.split("_", 1)[1].upper()
    except IndexError:
        symbol = lib_code.upper()

    base_dir = os.path.join("reports", symbol)
    type_subdir = "items" if kind == "items" else ("stats" if kind == "stats" else "patrons")
    local_dir = os.path.join(base_dir, type_subdir)
    os.makedirs(local_dir, exist_ok=True)

    # Connect to SFTP using shared utility
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
            else:
                print(f"Already exists: {filename}")

        sftp.close()
        ssh.close()

    except paramiko.SSHException as e:
        if DISCOVERY and "Discovery complete" in str(e):
            print("\nDiscovery mode completed successfully!")
        else:
            print("SSH connection failed:", e)

if __name__ == "__main__":
    args = parse_args()

    if args.print_fingerprint:
        print_server_fingerprint()
        
    else:
        # Nomal mode - require credentials from lib_code
        if not args.lib_code:
            print("Error: lib_code is required when not using --print_fingerprint")
            print("Example: python data_fetcher.py wx_wvb")
            exit(1)

        download_reports(args.lib_code, args.recent, args.stats, args.patrons, args.since)