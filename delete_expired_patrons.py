#!/usr/bin/env python3
"""
OCLC patron-file fetcher + delete-file generator (with expiration filtering)
-----------------------------------------------------------------------------
- Downloads the single latest full patron file from OCLC SFTP
- Filters patrons whose Patron_Expiration_Date is earlier than cutoff date
- Generates a tab-delimited delete file (.txt) with institutionId, barcode, sourceSystem, idAtSource, illId
- barcode is taken from Patron_Barcode exactly as-is (preserve leading/trailing zeroes, alphanumeric)
- institutionId for library is loaded from env var (e.g., ACACL_INSTITUTION_ID=1234)
- sourceSystem and idAtSource left blank in delete file per request
- For testing, saves delete file locally; can upload with --upload flag
"""

import os
import re
import sys
import csv
import argparse
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from dotenv import load_dotenv

# Import shared utilities
from sftp_utils import get_credentials, connect_sftp, list_remote_files, download_file
from file_utils import safe_read_txt, load_headers, extract_first_part_from_pipe_delimited
from data_loader import upload_patron_delete

# Load environment variables
load_dotenv()

# Set up logging
logger = logging.getLogger(__name__)


def build_parser():
    """Build argument parser with all CLI options."""
    p = argparse.ArgumentParser(
        description="Fetch latest weekly patron file, filter expired, build delete file"
    )
    p.add_argument("lib_code", help="Library code (e.g., wx_kqy)")
    p.add_argument("--remote-dir", default="/xfer/wms/reports",
                   help="Remote SFTP directory for patron files")
    p.add_argument("--output-dir", default="patrons",
                   help="Local base output directory")
    p.add_argument("--headers-file", default="headers_deletes.txt",
                   help="Path to delete file headers")
    p.add_argument("--barcode-column", default="Patron_Barcode",
                   help="Column name for patron barcode")
    p.add_argument("--expiration-column", default="Patron_Expiration_Date",
                   help="Column name for expiration date")
    p.add_argument("--expiration-date", type=str,
                   help="Custom expiration cutoff date (YYYY-MM-DD). Default: today")
    p.add_argument("--sync-illid-to-barcode", action="store_true",
                   help="Copy barcode to illId field (required for Tipasa libraries only)")
    p.add_argument("--use-source-value", action="store_true",
                        help="Extract FIRST part of pipe-delimited source fields (IdM values)")
    p.add_argument("--offline", action="store_true", help="Skip SFTP download and use existing file in patrons/downloads")
    p.add_argument("--upload", action="store_true",
                   help="Upload delete file to /xfer/wms/in/pdelete")
    return p


def derive_config_from_lib_code(lib_code: str) -> tuple[str, re.Pattern, str]:
    """
    Extract symbol from lib_code and derive configuration values.
    
    For lib_code like 'wx_acacl', extracts 'ACACL' as the symbol.
    
    Args:
        lib_code: Library code (e.g., 'wx_acacl', 'wx_kqy')
        
    Returns:
        Tuple of (institution_id_env, pattern, symbol)
        - institution_id_env: environment variable name (e.g., 'KQY_INSTITUTION_ID')
        - pattern: compiled regex pattern for matching files
        - symbol: the extracted symbol for file naming (e.g., 'KQY')
    """
    # Split by underscore and take the last part, convert to uppercase
    parts = lib_code.split('_')
    if len(parts) < 2:
        raise ValueError(
            f"lib_code '{lib_code}' should contain underscore (e.g., wx_kqy)"
        )
    
    symbol = parts[-1].upper()  # 'KQY' from 'wx_kqy'
    institution_id_env = f"{symbol}_INSTITUTION_ID"
    
    # Pattern matches .txt extensions
    pattern = re.compile(rf"^{symbol}\.Circulation_Patron_Report_Full\.(\d{{8}})\.txt$")
    
    return institution_id_env, pattern, symbol


def pick_latest(files: list[str], pattern: re.Pattern) -> str:
    """
    Pick the most recent file matching the pattern.
    
    Args:
        files: List of filenames
        pattern: Compiled regex pattern with date capture group
        
    Returns:
        Filename of most recent matching file
        
    Raises:
        FileNotFoundError: If no matching files found
    """
    candidates = []
    for f in files:
        m = pattern.match(f)
        if m:
            try:
                dt = datetime.strptime(m.group(1), "%Y%m%d")
                candidates.append((f, dt))
            except ValueError:
                continue
    
    if not candidates:
        raise FileNotFoundError(
            "No matching full patron files found with the expected naming pattern."
        )
    
    return max(candidates, key=lambda x: x[1])[0]


def parse_expiration_date(date_str: Optional[str]) -> pd.Timestamp:
    """
    Parse expiration date string or return today.
    
    Args:
        date_str: Date string in YYYY-MM-DD format, or None
        
    Returns:
        Pandas Timestamp normalized to midnight
        
    Raises:
        ValueError: If date string is invalid format
    """
    if date_str is None:
        return pd.Timestamp.today().normalize()
    
    try:
        return pd.to_datetime(date_str).normalize()
    except Exception as e:
        raise ValueError(
            f"Invalid date format: '{date_str}'. Use YYYY-MM-DD (e.g., 2025-01-15)"
        ) from e


def generate_delete_file(
    patron_file_path: Path,
    headers: list[str],
    barcode_col: str,
    expiration_col: str,
    institution_id: str,
    cutoff_date: pd.Timestamp,
    out_dir: Path,
    symbol: str,
    sync_illid: bool = False,
    use_source_value: bool = False
) -> Path:
    """
    Generate delete file from patron file, filtering by expiration date.
    
    Args:
        patron_file_path: Path to downloaded patron data
        headers: List of output column headers
        barcode_col: Name of barcode column
        expiration_col: Name of expiration date column
        institution_id: Institution ID value
        cutoff_date: Delete patrons expiring before this date
        out_dir: Output directory
        symbol: Library symbol for filename
        sync_illid: If True, copy barcode to illId for Tipasa libraries
        use_source_value: If True, copy userIdAtSource and sourceSystem
        
    Returns:
        Path to generated delete file
        
    Raises:
        KeyError: If required columns not found in file
    """
    # Read delimited text preserving all strings exactly (including leading zeros)
    df = safe_read_txt(patron_file_path)
    
    # Verify required columns exist
    if expiration_col not in df.columns:
        raise KeyError(f"Column not found in text: {expiration_col}")
    if barcode_col not in df.columns:
        raise KeyError(f"Column not found in text: {barcode_col}")
    
    # Parse expiration dates and filter
    df[expiration_col] = pd.to_datetime(df[expiration_col], errors="coerce").dt.normalize()
    expired_df = df[df[expiration_col] < cutoff_date]
    
    logger.info(f"Text rows: {len(df):,}; expired patrons: {len(expired_df):,}")
    logger.info(f"Cutoff date: {cutoff_date.strftime('%Y-%m-%d')}")
    
    # Build output records
    out_rows = []
    for _, row in expired_df.iterrows():
        rec = {}
        for h in headers:
            if h == "institutionId":
                rec[h] = institution_id
            elif h == "barcode":
                rec[h] = row.get(barcode_col, "")
            elif h == "sourceSystem":
                if use_source_value and "Patron_Source_System" in row:
                    raw_value = row.get("Patron_Source_System", "")
                    rec[h] = extract_first_part_from_pipe_delimited(raw_value, "sourceSystem")
                else:
                    rec[h] = ""
            elif h == "idAtSource":
                # Extract first part of pipe-delimited value if flag is set
                if use_source_value and "Patron_User_ID_At_Source" in row:
                    raw_value = row.get("Patron_User_ID_At_Source", "")
                    rec[h] = extract_first_part_from_pipe_delimited(raw_value, "idAtSource")
                else:
                    rec[h] = ""
            elif h == "illId":
                # Only populate illId for Tipasa libraries
                if sync_illid:
                    rec[h] = row.get(barcode_col, "")
                else:
                    rec[h] =""
            else:
                rec[h] = ""
        out_rows.append(rec)
    
    # Create output DataFrame
    out_df = pd.DataFrame(out_rows, columns=headers)
    
    # Ensure output directory exists
    out_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate filename: {symbol}patronsdelete_mmddyy.txt
    date_str = cutoff_date.strftime("%m%d%y")
    out_name = f"{symbol}patronsdelete_{date_str}.txt"
    out_path = out_dir / out_name
    
    # Write tab-delimited file
    out_df.to_csv(out_path, sep="\t", index=False, quoting=csv.QUOTE_MINIMAL)
    
    logger.info(f"Delete file created: {out_path}")
    return out_path


def main():
    """Main execution function."""
    args = build_parser().parse_args()
    
    # Derive configuration from lib_code
    institution_id_env, pattern, symbol = derive_config_from_lib_code(args.lib_code)
    
    # Set up logging with timestamp
    date_str = datetime.today().strftime("%m%d%y")
    log_name = f"{symbol}patronsdelete_{date_str}.log"
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
    
    try:
        logger.info("="*60)
        logger.info("Starting delete expired patrons process")
        logger.info("="*60)
        logger.info(f"Library code: {args.lib_code}")
        logger.info(f"Symbol: {symbol}")
        logger.info(f"Institution ID env var: {institution_id_env}")
        logger.info(f"File pattern: {pattern.pattern}")
        
        # Parse expiration cutoff date
        cutoff_date = parse_expiration_date(args.expiration_date)
        if args.expiration_date:
            logger.info(f"Using custom expiration cutoff: {cutoff_date.strftime('%Y-%m-%d')}")
        else:
            logger.info(f"Using today as expiration cutoff: {cutoff_date.strftime('%Y-%m-%d')}")
        
        # Set up paths
        output_dir = Path(args.output_dir)
        downloads_dir = output_dir / "downloads"
        processed_dir = output_dir / "deletes"
        headers_file = Path(args.headers_file)
        
# Handle file download (online or offline mode)
        if args.offline:
            # Use existing file in downloads directory
            logger.info("Offline mode: looking for existing file in downloads directory")

            if not downloads_dir.exists():
                raise FileNotFoundError("Downloads directory does not exist. Run without --offline first.")
            
            # Find the most recent patron file matching the symbol
            patron_files = []
            for file_path in downloads_dir.glob(f"{symbol}.Circulation_Patron_Report_Full.*.txt"):
                m = pattern.match(file_path.name)
                if m:
                    date_str = m.group(1)
                    try:
                        file_date = datetime.strptime(date_str, "%Y%m%d")
                        patron_files.append((file_path, file_date))
                    except ValueError:
                        continue

            if not patron_files:
                raise FileNotFoundError(f"No patron files found for {symbol} in downloads directory")
            
            # Get the most recent file
            patron_local, _ = max(patron_files, key=lambda x: x[1])
            logger.info(f"Using existing file: {patron_local}")

        else:
            # Online mode: Connect to SFTP and download
            user, pwd = get_credentials(args.lib_code)
            ssh, sftp = connect_sftp(user, pwd, verify=True)
            
            try:
                # List files and find latest
                logger.info(f"Searching for files in {args.remote_dir}")
                files = list_remote_files(sftp, args.remote_dir)
                latest = pick_latest(files, pattern)
                logger.info(f"Found latest file: {latest}")
                
                # Download patron file
                patron_local = download_file(sftp, args.remote_dir, latest, downloads_dir)
                logger.info(f"Downloaded to: {patron_local}")
                
            finally:
                sftp.close()
                ssh.close()
                logger.info("SSH connection closed")
        
        # Continue processing (runs for both offline and online modes)
        # Get institution ID from environment
        institution_id = os.getenv(institution_id_env, "")
        if not institution_id:
            logger.error(
                f"Institution ID is REQUIRED for delete operations. "
                "Set %s in .env file (e.g., %s=12345)",
                institution_id_env, institution_id_env
            )
            sys.exit(1)

        logger.info(f"Institution ID: {institution_id}")
        
        # Load headers
        headers = load_headers(headers_file)
        logger.info(f"Loaded headers from {headers_file}: {', '.join(headers)}")
        
        # Generate delete file
        delete_path = generate_delete_file(
            patron_file_path=patron_local,
            headers=headers,
            barcode_col=args.barcode_column,
            expiration_col=args.expiration_column,
            institution_id=institution_id,
            cutoff_date=cutoff_date,
            out_dir=processed_dir,
            symbol=symbol,
            sync_illid=args.sync_illid_to_barcode,
            use_source_value=args.use_source_value
        )
        
        logger.info(f"Delete file ready: {delete_path}")
        
        # Upload if requested
        if args.upload:
            logger.info("Uploading delete file...")
            remote_path = upload_patron_delete(
                args.lib_code,
                delete_path,
                verify_fingerprint=True,
                require_confirmation=True
            )
            if remote_path:
                logger.info(f"Upload completed successfully: {remote_path}")
            else:
                logger.info("Upload cancelled by user")
        else:
            logger.info("No upload requested (use --upload flag to upload)")
        
        logger.info("="*60)
        logger.info("Process completed successfully")
        logger.info("="*60)
        
    except Exception as e:
        logger.exception(f"Unhandled error: {e}")
        raise

if __name__ == "__main__":
    main()
