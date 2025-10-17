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
import sys
import re
import csv
import argparse
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple
from collections import Counter

import pandas as pd
import paramiko

from sftp_utils import get_credentials, connect_sftp, list_remote_files, download_file
from file_utils import safe_read_txt, extract_first_part_from_pipe_delimited, analyze_pipe_delimited_patterns
from data_loader import upload_patron_reload

from dotenv import load_dotenv

# Set up module-level logger (configured in main())
logger = logging.getLogger(__name__)

# -----------------------------
# SFTP + general helpers
# -----------------------------

load_dotenv()

EXPECTED_FINGERPRINT = os.getenv("FINGERPRINT", "")
HOST = os.getenv("HOST_NAME", "")
PORT = int(os.getenv("HOST_PORT", "22"))


def get_expiration_date() -> Optional[str]:
    """Get EXPIRATION_DATE from .env file if set and not 'IGNORE'."""
    exp_date = os.getenv("EXPIRATION_DATE", "").strip()
    if not exp_date or exp_date.upper() == "IGNORE":
        return None
    return exp_date


def download_patron_file_any_extension(sftp: paramiko.SFTPClient, remote_dir: str, base_pattern: str, downloads_dir: Path) -> Tuple[Path, str]:
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
    files = download_file(sftp, remote_dir)
    
    # Try .txt first, then .csv
    extensions_to_try = ['.txt', '.csv']
    
    for ext in extensions_to_try:
        logger.info(f"Looking for files with {ext} extension...")
        
        # Modify pattern to use current extension
        if base_pattern.endswith(r'\.txt\$'):
            current_pattern = base_pattern.replace(r'\.txt\$', f'\\{ext}$')
        else:
            # Assume the pattern ends with some extension, replace it
            current_pattern = re.sub(r'\\\.[a-zA-Z]+\$$', f'\\{ext}$', base_pattern)
        
        logger.info(f"Using pattern: {current_pattern}")
        pattern = re.compile(current_pattern)
        
        try:
            latest_name, symbol = pick_latest_full_patron(files, pattern)
            logger.info(f"Found matching file: {latest_name}")
            
            # Try to download
            txt_local = download_file(sftp, remote_dir, latest_name, downloads_dir)
            
            # Verify the file has content
            if txt_local.stat().st_size > 0:
                logger.info(f"Successfully downloaded {latest_name} with {ext} extension")
                return txt_local, symbol
            else:
                logger.warning(f"File {latest_name} was downloaded but is empty")
                
        except FileNotFoundError as e:
            logger.warning(f"No matching {ext} files found: {e}")
            continue
        except Exception as e:
            logger.warning(f"Failed to download {ext} file: {e}")
            continue
    
    # If we get here, neither extension worked
    raise FileNotFoundError(f"Could not download patron file with any extension (.txt, .csv)")

# Updated pattern that can work with both extensions
def get_flexible_pattern():
    """Return a pattern that can be easily modified for different extensions"""
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
        raise FileNotFoundError("No matching full patron files found with the expected naming pattern.")
    f, _, sym = max(candidates, key=lambda x: x[1])
    return f, sym

def load_headers(headers_file: Path) -> list:
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
    parts = lib_code.split('_')
    if len(parts) >= 2:
        symbol = parts[-1].upper()
    else:
        symbol = lib_code.upper()
    
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
    try:
        df = pd.read_csv(txt_path, dtype=str, nrows=50)
    except Exception:
        return None
    for col in df.columns:
        if col.lower() == "inst_symbol":
            vals = df[col].dropna().astype(str).str.strip()
            if not vals.empty:
                return vals.mode().iat[0] if not vals.mode().empty else vals.iloc[0]
    return None


def load_patron_updates(project_root: Path) -> Optional[pd.DataFrame]:
    """
    Load patron_updates.txt (or barcode_updates.txt for backward compatibility).
    Supports updating multiple fields: barcode, emailAddress, or other patron fields.
    
    Required column: patron_barcode_old (to identify which patrons to update)
    Optional columns: patron_barcode_new, emailAddress, borrowerCategory, etc.
    
    Trims whitespace and ensures dtype=str.
    """
    # Try new name first, fall back to old name
    new_path = project_root / "patron_updates.txt"
    old_path = project_root / "barcode_updates.txt"
    
    if new_path.exists():
        p = new_path
        logger.info("Loading patron updates from patron_updates.txt")
    elif old_path.exists():
        p = old_path
        logger.info("Loading patron updates from barcode_updates.txt (consider renaming to patron_updates.txt)")
    else:
        return None
    
    df = pd.read_csv(p, sep="\t", dtype=str).fillna("")
    
    # patron_barcode_old is required to identify records
    if "patron_barcode_old" not in df.columns:
        raise ValueError(f"{p.name} must contain 'patron_barcode_old' column to identify patrons")
    
    # Trim whitespace on all columns
    for col in df.columns:
        before = df[col].astype(str)
        after = before.str.strip()
        trimmed = (before != after).sum()
        if trimmed:
            logger.info(f"Trimmed whitespace on {trimmed} '{col}' values from {p.name}")
        df[col] = after
    
    # Log what updates will be applied
    update_cols = [c for c in df.columns if c != "patron_barcode_old"]
    logger.info(f"Patron updates file contains {len(df)} rows with updates for: {', '.join(update_cols)}")
    
    return df

# -----------------------------
# Validation helpers
# -----------------------------

RESERVED_URL_CHARS = set("!*'();:@&=+$,/?%#[]")


def utf8_len_bytes(s: str) -> int:
    return len(s.encode("utf-8"))

def preflight_validate_updates(
    updates_df: pd.DataFrame,
    existing_barcodes_in_file: set,
    soft_max_chars: int,
    hard_max_bytes: int,
) -> pd.DataFrame:
    """
    Validate new barcodes against OCLC requirements (only if barcode updates present).
    - non-empty (warn and drop empty)
    - <= soft_max_chars (warn only)
    - <= hard_max_bytes (warn only; 30 bytes aligns with WMS item guidance)
    - duplicates within updates (warn)
    - new barcode appears elsewhere in incoming file (warn)
    - reserved URL characters present (warn)
    Returns a potentially filtered copy (empty 'new' values removed).
    """
    if updates_df is None or updates_df.empty:
        return updates_df

    df = updates_df.copy()

    # Non-empty
    empty_new = df["patron_barcode_new"].eq("")
    if empty_new.any():
        n = int(empty_new.sum())
        logger.warning("barcode_updates: %d rows have EMPTY patron_barcode_new; they will be ignored.", n)
        df = df.loc[~empty_new].copy()

    # Soft and hard limits
    too_long_soft = df["patron_barcode_new"].str.len() > soft_max_chars
    if too_long_soft.any():
        logger.warning(
            "barcode_updates: %d 'new' barcodes exceed soft max of %d characters (recommend shortening).",
            int(too_long_soft.sum()), soft_max_chars,
        )

    too_long_hard = df["patron_barcode_new"].map(utf8_len_bytes) > hard_max_bytes
    if too_long_hard.any():
        logger.warning(
            "barcode_updates: %d 'new' barcodes exceed %d BYTES (WMS barcode limit guidance).",
            int(too_long_hard.sum()), hard_max_bytes,
        )

    # Duplicates within the change list
    dupe_mask = df["patron_barcode_new"].duplicated(keep=False)
    if dupe_mask.any():
        sample = sorted(df.loc[dupe_mask, "patron_barcode_new"].unique())[:10]
        logger.warning("barcode_updates: duplicate 'new' barcode values detected (sample): %s", ", ".join(sample))

    # New barcode already exists in incoming file (possible collision)
    if existing_barcodes_in_file:
        olds = set(df["patron_barcode_old"])
        collisions = sorted([b for b in df["patron_barcode_new"] if b and (b in existing_barcodes_in_file and b not in olds)])
        if collisions:
            logger.warning(
                "barcode_updates: %d 'new' barcodes already exist in the incoming patron file (sample): %s",
                len(collisions), ", ".join(collisions[:10])
            )

    # Reserved URL characters
    has_reserved = df["patron_barcode_new"].map(lambda s: any(ch in RESERVED_URL_CHARS for ch in s))
    if has_reserved.any():
        sample = df.loc[has_reserved, "patron_barcode_new"].head(10).tolist()
        logger.warning(
            "barcode_updates: %d 'new' barcodes include reserved URL characters (sample): %s",
            int(has_reserved.sum()), ", ".join(sample)
        )

    return df

# Helper for sourceSystem (most common)
def find_most_common_source_system(df: pd.DataFrame, column_name: str) -> str:
    """
    Find the most common sourceSystem value (URN, URL, or other identifier).
    This will be used as the default for all rows.
    """
    if column_name not in df.columns:
        return ""
    
    # Extract all non-empty sourceSystem values
    source_systems = []
    for value in df[column_name]:
        extracted = extract_first_part_from_pipe_delimited(str(value))
        if extracted:
            source_systems.append(extracted)
    
    if not source_systems:
        logger.warning("No sourceSystem values found")
        return ""
    
    # Find most common value
    system_counts = Counter(source_systems)
    most_common_system = system_counts.most_common(1)[0][0]
    
    logger.info(f"Most common sourceSystem: {most_common_system} "
                f"(appears {system_counts[most_common_system]} times)")
    return most_common_system

def process_special_fields(in_df: pd.DataFrame) -> pd.DataFrame:
    """
    Process idAtSource and sourceSystem fields to extract clean values.
    Handles pipe-delimited junk data by taking the first part.
    
    WARNING: If values have been updated over time, old values may appear
    first in the pipe-delimited list. REVIEW the log output and verify your
    output file before uploading!
    """
    df = in_df.copy()
    
    # Process idAtSource
    if "Patron_User_ID_At_Source" in df.columns:
        logger.info("Processing idAtSource field...")
        
        # Analyze the data first
        analyze_pipe_delimited_patterns(df, "Patron_User_ID_At_Source")
        
        # Extract first part
        df["Patron_User_ID_At_Source"] = df["Patron_User_ID_At_Source"].apply(
            lambda x: extract_first_part_from_pipe_delimited(x, "idAtSource")
        )
        
        # Show samples of results
        non_empty = df["Patron_User_ID_At_Source"][df["Patron_User_ID_At_Source"] != ""]
        if not non_empty.empty:
            logger.info(f"Sample extracted idAtSource values: {non_empty.head(3).tolist()}")
    
    # Process sourceSystem
    if "Patron_Source_System" in df.columns:
        logger.info("Processing sourceSystem field...")
        
        # Analyze the data first
        analyze_pipe_delimited_patterns(df, "Patron_Source_System")
        
        # Find most common first-part value
        most_common_value = find_most_common_source_system(df, "Patron_Source_System")
        if most_common_value:
            df["Patron_Source_System"] = most_common_value
            logger.info(f"Set all sourceSystem values to: {most_common_value}")
        else:
            logger.warning("No valid sourceSystem value found, leaving column empty")
            df["Patron_Source_System"] = ""
    
    return df

def extract_email_from_field(value: str, valid_domains: list) -> Optional[str]:
    """
    Extract the first email matching any of the valid domains from a text field.
    Case-insensitive domain matching.
    
    Args:
        value: Text field that may contain email addresses
        valid_domains: List of valid domains (e.g., ['@bethanywv.edu', '@bethany.edu'])
        
    Returns:
        First matching email address (lowercase), or None if no match found
        
    Examples:
        extract_email_from_field('jsmith@bethanywv.edu', ['@bethanywv.edu']) 
        -> 'jsmith@bethanywv.edu'
        
        extract_email_from_field('Contact: Jane.Doe@BETHANYWV.EDU for info', ['@bethanywv.edu'])
        -> 'jane.doe@bethanywv.edu'
    """
    if pd.isna(value) or value == "":
        return None
    
    value_str = str(value).lower()  # Convert to lowercase for case-insensitive matching
    
    # Check each valid domain
    for domain in valid_domains:
        domain_lower = domain.lower()
        if domain_lower in value_str:
            # Simple email extraction: find text before and after the domain
            # Look for the @ symbol and extract word characters around it
            import re
            # Pattern: word chars, dots, hyphens before @ + domain
            pattern = r'[\w\.-]+' + re.escape(domain_lower)
            match = re.search(pattern, value_str)
            if match:
                return match.group(0)
    
    return None


def filter_patrons_by_criteria(
    df: pd.DataFrame,
    valid_domains: list,
    barcode_col: str = "Patron_Barcode",
    email_col: str = "Patron_Email_Address",
    username_col: str = "Patron_Username",
    expiration_col: str = "Patron_Expiration_Date",
    check_uniqueness: bool = True
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Filter patrons by expiration status and email domain.
    
    Filters applied in order:
    1. Remove expired patrons (expiration date < today)
    2. Check email_col for valid domain, fallback to username_col
    3. Remove patrons without valid email match
    4. Remove patrons with non-unique emails (if check_uniqueness=True)
    
    Args:
        df: Input patron DataFrame
        valid_domains: List of valid email domains to match
        barcode_col: Name of barcode column
        email_col: Name of primary email column
        username_col: Name of username column (fallback for email)
        expiration_col: Name of expiration date column
        check_uniqueness: If True, filter out patrons with duplicate emails
        
    Returns:
        Tuple of (filtered_df, skipped_df)
        - filtered_df: Patrons that passed all filters with new 'matched_email' column
        - skipped_df: Patrons that were filtered out with 'skip_reason' column
    """
    logger.info("="*60)
    logger.info("Filtering patrons by criteria")
    logger.info("="*60)
    logger.info(f"Starting with {len(df):,} total patrons")
    logger.info(f"Valid email domains: {', '.join(valid_domains)}")
    
    # Track skipped records
    skipped_records = []
    
    # Make a copy to avoid modifying original
    working_df = df.copy()
    
    # Step 1: Filter out expired patrons
    if expiration_col in working_df.columns:
        # Parse expiration dates
        working_df[expiration_col] = pd.to_datetime(
            working_df[expiration_col], 
            errors="coerce"
        ).dt.normalize()
        
        today = pd.Timestamp.today().normalize()
        
        # Find expired patrons
        expired_mask = working_df[expiration_col] < today
        
        if expired_mask.any():
            # Record skipped expired patrons
            for _, row in working_df[expired_mask].iterrows():
                skipped_records.append({
                    'barcode': row.get(barcode_col, ''),
                    'familyName': row.get('Patron_Family_Name', ''),
                    'givenName': row.get('Patron_Given_Name', ''),
                    'email': row.get(email_col, ''),
                    'skip_reason': f"Expired: {row.get(expiration_col, 'N/A')}"
                })
            
            logger.info(f"Filtered out {expired_mask.sum():,} expired patrons")
            working_df = working_df[~expired_mask].copy()
    else:
        logger.warning(f"Column '{expiration_col}' not found - skipping expiration filter")
    
    logger.info(f"After expiration filter: {len(working_df):,} patrons")
    
    # Step 2: Find valid email for each patron
    working_df['matched_email'] = None
    
    for idx, row in working_df.iterrows():
        matched_email = None
        
        # First try primary email column
        if email_col in working_df.columns:
            matched_email = extract_email_from_field(
                row.get(email_col, ''), 
                valid_domains
            )
        
        # If no match, try username column
        if not matched_email and username_col in working_df.columns:
            matched_email = extract_email_from_field(
                row.get(username_col, ''),
                valid_domains
            )
        
        # Store the matched email (or None)
        working_df.at[idx, 'matched_email'] = matched_email
        
        # Track if no match found
        if not matched_email:
            skipped_records.append({
                'barcode': row.get(barcode_col, ''),
                'familyName': row.get('Patron_Family_Name', ''),
                'givenName': row.get('Patron_Given_Name', ''),
                'email': row.get(email_col, ''),
                'skip_reason': 'No valid email domain found'
            })
    
    # Step 3: Keep only patrons with matched emails
    has_email_mask = working_df['matched_email'].notna()
    
    if not has_email_mask.any():
        logger.error("No patrons matched the email domain criteria!")
        logger.error(f"Checked domains: {', '.join(valid_domains)}")
        raise ValueError(
            f"No patrons found with email domains: {', '.join(valid_domains)}"
        )
    
    logger.info(f"Matched valid email for {has_email_mask.sum():,} patrons")
    logger.info(f"Filtered out {(~has_email_mask).sum():,} patrons without valid email")
    
    working_df = working_df[has_email_mask].copy()
    
    # Step 4: Check for duplicate emails (generic/shared addresses)
    if check_uniqueness:
        logger.info("Checking for duplicate/shared email addresses...")
        
        # Count occurrences of each email
        email_counts = working_df['matched_email'].value_counts()
        duplicate_emails = email_counts[email_counts > 1]
        
        if not duplicate_emails.empty:
            logger.warning("="*60)
            logger.warning("DUPLICATE/SHARED EMAIL ADDRESSES FOUND")
            logger.warning("="*60)
            logger.warning(f"Found {len(duplicate_emails)} email addresses used by multiple patrons:")
            
            for email, count in duplicate_emails.items():
                logger.warning(f"  {email}: {count} patrons")
            
            logger.warning("These patrons will be SKIPPED (cannot use shared email as unique idAtSource)")
            logger.warning("="*60)
            
            # Filter out patrons with duplicate emails
            duplicate_mask = working_df['matched_email'].isin(duplicate_emails.index)
            
            # Record skipped patrons with duplicate emails
            for _, row in working_df[duplicate_mask].iterrows():
                email = row['matched_email']
                count = email_counts[email]
                skipped_records.append({
                    'barcode': row.get(barcode_col, ''),
                    'familyName': row.get('Patron_Family_Name', ''),
                    'givenName': row.get('Patron_Given_Name', ''),
                    'email': email,
                    'skip_reason': f"Shared/duplicate email (used by {count} patrons)"
                })
            
            logger.info(f"Filtered out {duplicate_mask.sum():,} patrons with shared emails")
            working_df = working_df[~duplicate_mask].copy()
    
    # Create skipped DataFrame
    skipped_df = pd.DataFrame(skipped_records)
    
    logger.info("="*60)
    logger.info(f"Final filtered count: {len(working_df):,} patrons")
    logger.info(f"Total skipped: {len(skipped_df):,} patrons")
    logger.info("="*60)
    
    return working_df, skipped_df

def write_skipped_patrons_report(skipped_df: pd.DataFrame, output_dir: Path, symbol: str) -> Optional[Path]:
    """
    Write a report of skipped patron records.
    
    Args:
        skipped_df: DataFrame with skipped patron records
        output_dir: Directory to write report
        symbol: Library symbol for filename
        
    Returns:
        Path to report file, or None if no records skipped
    """
    if skipped_df.empty:
        logger.info("No patrons were skipped - no report needed")
        return None
    
    # Create reports directory
    reports_dir = output_dir / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate filename with date
    date_str = datetime.today().strftime("%Y%m%d")
    report_name = f"{symbol}_skipped_patrons_{date_str}.txt"
    report_path = reports_dir / report_name
    
    # Write tab-delimited report
    skipped_df.to_csv(report_path, sep="\t", index=False)
    
    logger.info(f"Skipped patrons report written: {report_path}")
    logger.info(f"  - Total skipped: {len(skipped_df):,}")
    
    # Log breakdown by reason
    if 'skip_reason' in skipped_df.columns:
        reason_counts = skipped_df['skip_reason'].value_counts()
        logger.info("  - Breakdown by reason:")
        for reason, count in reason_counts.items():
            logger.info(f"      {reason}: {count:,}")
    
    return report_path

# -----------------------------
# Core processing
# -----------------------------

INCOMING_TO_FORMATTED_MAP = {
    "prefix": "prefix",
    "givenName": "Patron_Given_Name",
    "middleName": "middleName",
    "familyName": "Patron_Family_Name",
    "suffix": "suffix",
    "nickname": "nickname",
    # Excluded columns load as blank ""
    # Optional: add mappings here (final output column:change "" to column in incoming OCLC TXT)  
    # Optional: if mapped here, can add to field_mappings (patron_updates.txt column:incoming OCLC TXT column)
    "dateOfBirth": "",  # Excluded 
    "gender": "",  # Excluded 
    # institutionId computed
    "barcode": "Patron_Barcode",
    "idAtSource": "Patron_User_ID_At_Source",  # Will be processed with helper function
    "sourceSystem": "Patron_Source_System",  # Will be processed with helper function
    "borrowerCategory": "Patron_Borrower_Category", 
    "circRegistrationDate": "",  # Excluded
    "oclcExpirationDate": "",  # Excluded by default - but can be set with --use-expiration-date
    "homeBranch": "Patron_Home_Branch_ID",
    # Excluded all address/contact mappings except email address
    "primaryStreetAddressLine1": "",  # Excluded
    "primaryStreetAddressLine2": "",  # Excluded
    "primaryCityOrLocality": "",  # Excluded
    "primaryStateOrProvince": "",  # Excluded
    "primaryPostalCode": "",  # Excluded
    "primaryCountry": "",  # Excluded
    "primaryPhone": "",  # Excluded
    "secondaryStreetAddressLine1": "",  # Excluded
    "secondaryStreetAddressLine2": "",  # Excluded
    "secondaryCityOrLocality": "",  # Excluded
    "secondaryStateOrProvince": "",  # Excluded
    "secondaryPostalCode": "",  # Excluded
    "secondaryCountry": "",  # Excluded
    "secondaryPhone": "",  # Excluded
    "emailAddress": "Patron_Email_Address",
    "mobilePhone": "",  # Excluded
    "notificationEmail": "",  # Excluded
    "notificationTextPhone": "",  # Excluded
    "patronNotes": "",  # Excluded
    "photoURL": "",  # Excluded
    "customdata1": "",  # Excluded
    "customdata2": "",  # Excluded
    "customdata3": "",  # Excluded
    "customdata4": "",  # Excluded
    "username": "Patron_Username",
    "illId": "",  # Excluded by default - but can be set with --sync-illid-to-barcode
    "illApprovalStatus": "",  # Excluded
    "illPatronType": "",  # Excluded
    "illPickupLocation": "",  # Excluded
}

def build_formatted_df(
        in_df: pd.DataFrame, 
        headers: list, 
        institution_id: str, 
        can_self_edit: bool, 
        use_expiration_date: bool = False,
        use_source_value: bool = False
    ) -> pd.DataFrame:
    """Create a DataFrame with 46 columns in the exact order from headers."""
    
    # First, process special fields that need cleaning
    if use_source_value:
        logger.info("Processing source fields (--use-source-value enabled)")
        processed_df = process_special_fields(in_df)
    else:
        logger.info("Skipping source field processing (use --use-source-value to enable)")
        processed_df = in_df.copy()
    
    # Create output DataFrame with proper structure
    out = pd.DataFrame({h: "" for h in headers}, index=processed_df.index)

    # Direct mappings where available
    for formatted_col, incoming_col in INCOMING_TO_FORMATTED_MAP.items():
        if formatted_col not in out.columns:
            continue

        # Special handling for source fields
        if formatted_col in ("idAtSource", "sourceSystem"):
            # Priority: 1) Values from filtering, 2) --use-source-value flag, 3) empty
            if formatted_col == "idAtSource" and "matched_email" in processed_df.columns:
                # Use matched email as idAtSource if available
                out[formatted_col] = processed_df["matched_email"].astype(str).fillna("")
            elif formatted_col == "sourceSystem" and "source_system_value" in processed_df.columns:
                # Use specified source system value if available
                out[formatted_col] = processed_df["source_system_value"].astype(str).fillna("")
            elif use_source_value and incoming_col != "" and incoming_col in processed_df.columns:
                # Fall back to --use-source-value behavior
                out[formatted_col] = processed_df[incoming_col].astype(str).fillna("")
            else:
                out[formatted_col] = ""
            continue
            
        if incoming_col == "":
            # This field is excluded - leave as empty string
            out[formatted_col] = ""
        elif incoming_col in processed_df.columns:
            # Map the processed data
            out[formatted_col] = processed_df[incoming_col].astype(str).fillna("")
        else:
            # Column doesn't exist in input, leave empty
            out[formatted_col] = ""

    # Special-cases / computed fields
    # Check if canSelfEdit was provided in patron_updates.txt
    if "canSelfEdit" in processed_df.columns:
        # Use values from updates file where available, default for others
        out["canSelfEdit"] = processed_df["canSelfEdit"].replace("", "true" if can_self_edit else "false")
        updated_count = (processed_df["canSelfEdit"] != "").sum()
        if updated_count > 0:
            logger.info(f"Using canSelfEdit values from patron_updates.txt for {updated_count} rows")
    else:
        # No updates file or no canSelfEdit column - use default
        out["canSelfEdit"] = "true" if can_self_edit else "false"
    
    out["institutionId"] = str(institution_id) if institution_id is not None else ""

    # Handle illId if it was synced from barcode updates
    if "illId" in processed_df.columns:
        out["illId"] = processed_df["illId"].astype(str).fillna("")
        synced_count = (processed_df["illId"] != "").sum()
        if synced_count > 0:
            logger.info(f"Using synced illId values for {synced_count} rows")
    else:
        # illId wasn't synced, leave as excluded (empty)
        out["illId"] = ""

    # Handle expiration date
    if use_expiration_date:
        exp_date = get_expiration_date()
        if exp_date:
            logger.info(f"Setting oclcExpirationDate to: {exp_date}")
            out["oclcExpirationDate"] = exp_date
        else:
            logger.info("EXPIRATION_DATE not found or set to IGNORE, leaving oclcExpirationDate empty")
            out["oclcExpirationDate"] = ""
    else:
        out["oclcExpirationDate"] = ""

    # Ensure all columns are strings, preserve leading zeros
    for c in out.columns:
        out[c] = out[c].astype(str).fillna("")

    return out

def apply_patron_updates_if_any(
    in_df: pd.DataFrame,
    updates_df: Optional[pd.DataFrame],
    sync_illid: bool = False,
) -> pd.DataFrame:
    
    """
    Apply patron updates from patron_updates.txt.
    
    Required fields (familyName, barcode, borrowerCategory, homeBranch, emailAddress) 
    are ALWAYS updated, even if blank in patron_updates.txt.
    
    Optional fields (givenName, username, etc.) are only updated if non-blank,
    preserving the original OCLC value when blank.
    """
    if updates_df is None:
        return in_df

    if "Patron_Barcode" not in in_df.columns:
        logger.warning("Incoming file missing Patron_Barcode column; cannot apply updates")
        return in_df

    # Merge on patron_barcode_old
    merged = in_df.merge(
        updates_df,
        left_on="Patron_Barcode",
        right_on="patron_barcode_old",
        how="inner",
        suffixes=("", "_upd"),
        copy=False,
    )

    if merged.empty:
        logger.warning("No matching barcodes found in patron_updates.txt.")
        logger.warning("This may be because filtered patrons don't match the update file.")
        logger.warning("Continuing with filtered patrons (no updates applied).")
        # Return the original df unchanged
        return in_df

    num_matched = len(merged)
    logger.info(f"Matched {num_matched} patron rows to update")

    # ==========================================
    # APPLY FIELD UPDATES - Only update if non-blank
    # ==========================================
    
    # Barcode (special case: can be same as old, handled separately)
    if "patron_barcode_new" in updates_df.columns:
        merged["Patron_Barcode"] = merged["patron_barcode_new"].astype(str)
        logger.info("Updated Patron_Barcode for all matched rows")
        
        if sync_illid:
            if "illId" not in merged.columns:
                merged["illId"] = ""
            merged["illId"] = merged["Patron_Barcode"]
            logger.info("Synced illId to updated barcode")
    
    # Define all field mappings (update file column → OCLC data column)
    field_mappings = {
        "familyName": "Patron_Family_Name",
        "givenName": "Patron_Given_Name",
        "borrowerCategory": "Patron_Borrower_Category",
        "homeBranch": "Patron_Home_Branch_ID",
        "emailAddress": "Patron_Email_Address",
        "username": "Patron_Username",
        "illId": "illId"
        # Add other fields as needed
    }
    
    # Update all fields using the same logic: only update if non-blank
    for update_col, patron_col in field_mappings.items():
        if update_col in updates_df.columns and patron_col in merged.columns:
            # Only update rows where the update value is non-blank
            update_mask = merged[update_col].astype(str).str.strip() != ""
            if update_mask.any():
                merged.loc[update_mask, patron_col] = merged.loc[update_mask, update_col].astype(str)
                updated_count = update_mask.sum()
                logger.info(f"Updated {patron_col} for {updated_count} rows (preserved OCLC values where update was blank)")
            else:
                logger.info(f"No non-blank updates found for {patron_col}, preserving all OCLC values")
    
    # ==========================================
    # SPECIAL HANDLING - canSelfEdit
    # ==========================================
    
    # Handle canSelfEdit specially - it doesn't exist in incoming TXT
    # but needs to be preserved for build_formatted_df() to use
    if "canSelfEdit" in updates_df.columns:
        # Normalize to lowercase true/false for consistency
        merged["canSelfEdit"] = merged["canSelfEdit"].astype(str).str.lower()
        # Ensure only valid values
        valid_mask = merged["canSelfEdit"].isin(["true", "false"])
        if not valid_mask.all():
            invalid_count = (~valid_mask).sum()
            logger.warning(f"{invalid_count} canSelfEdit values are not 'true' or 'false', will use default")
            merged.loc[~valid_mask, "canSelfEdit"] = ""
        logger.info("Set canSelfEdit from patron_updates.txt for matched rows")
    
    # Keep only original columns (drop the _upd and update-file columns)
    keep_cols = [c for c in merged.columns if c in in_df.columns]
    
    # Preserve special columns that aren't in the incoming file but need to flow through
    special_cols = ["canSelfEdit", "illId"]
    for col in special_cols:
        if col in merged.columns and col not in in_df.columns:
            keep_cols.append(col)
    
    return merged[keep_cols]

def main(argv=None):
    p = argparse.ArgumentParser(description="Build OCLC-formatted patron reload file, with optional patron updates (barcode, email, etc.).")
    p.add_argument("lib_code", help="Credential key (e.g., wx_acacl). Used to find <LIB_CODE>_USER/<LIB_CODE>_PASS env vars.")
    p.add_argument("--offline", action="store_true", help="Skip SFTP download and use existing file in patronloads/downloads")
    p.add_argument("--remote-dir", default="/xfer/wms/reports", help="Remote reports directory")
    p.add_argument("--upload", action="store_true", help="If set, upload the result to /xfer/wms/in/patron")
    p.add_argument("--upload-test", action="store_true", help="If set, upload to TEST directory /xfer/wms/test/in/patron")
    p.add_argument("--output-dir", default="patrons", help="Local base output directory")
    p.add_argument("--headers-file", default="headers_formattedpatron.txt", help="Path to headers (46 columns)")
    p.add_argument("--project-root", default=".", help="Project root where patron_updates.txt (or barcode_updates.txt) may exist")
    p.add_argument("--can-self-edit", default="false", choices=["true", "false"], help="Default value for canSelfEdit")
    p.add_argument("--pattern", default=r"^([A-Z]{3})\.Circulation_Patron_Report_Full\.(\d{8})\.txt$", help="Regex to match patron full files")
    p.add_argument("--soft-max-barcode-len", type=int, default=int(os.getenv("SOFT_MAX_BARCODE_LEN", 20)),
                        help="Soft character limit for 'new' barcodes (warn if exceeded). Default 20.")
    p.add_argument("--hard-max-barcode-bytes", type=int, default=int(os.getenv("HARD_MAX_BARCODE_BYTES", 30)),
                        help="Hard byte-size guidance for WMS barcodes (warn if exceeded). Default 30.")
    p.add_argument("--sync-illid-to-barcode", action="store_true",
                        help="If set, copy updated barcodes into illId for matched rows.")
    p.add_argument("--use-expiration-date", action="store_true",
                        help="If set, apply EXPIRATION_DATE from .env to oclcExpirationDate field. Either do not enable or enter 'IGNORE' in .env to skip.")
    p.add_argument("--use-source-value", action="store_true",
                        help="Extract FIRST part of pipe-delimited source fields (IdM values)")
    p.add_argument("--filter-email-domain", type=str, action="append",
                        help="Only reload patrons with email containing this domain (e.g., @bethanywv.edu). Can be specified multiple times for multiple domains.")
    p.add_argument("--set-idsource-from-email", action="store_true",
                        help="Use the matched email address as idAtSource (lowercase). Requires --filter-email-domain.")
    p.add_argument("--source-system", type=str,
                        help="Set sourceSystem to this value for all reloaded patrons (e.g., https://sts.windows.net/...)")
    args = p.parse_args(argv)

    parts = args.lib_code.split('_')
    if len(parts) >= 2:
        symbol = parts[-1].upper()
    else:
        symbol = args.lib_code.upper()
    
    # Set up logging with timestamp
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
      
    logger.info("="*60)
    logger.info("Starting patron reload process")
    logger.info("="*60)
    logger.info(f"Library code: {args.lib_code}")
    logger.info(f"Symbol: {symbol}")    

    # Resolve paths
    output_dir = Path(args.output_dir)
    downloads_dir = output_dir / "downloads"
    processed_dir = output_dir / "reloads"
    headers_file = Path(args.headers_file)
    project_root = Path(args.project_root)

    # Skip sFTP
    if args.offline:
        # Use existing file in downloads directory
        logger.info("Offline mode: looking for existing file in downloads directory")
        downloads_dir = output_dir / "downloads"

        if not downloads_dir.exists():
            raise FileNotFoundError("Downloads directory does not exist. Run without --offline first.")
        
        # Determine the expected symbol from lib_code
        # For lib_code like 'wx_acacl', extract 'ACACL'
        parts = args.lib_code.split('_')
        if len(parts) < 2:
            raise ValueError(f"lib_code '{args.lib_code}' should contain underscore (e.g., wx_acacl)")
        expected_symbol = parts[-1].upper()
        
        logger.info(f"Looking for files matching symbol: {expected_symbol}")
        
        # Find the most recent patron file for THIS library
        patron_files = []
        for ext in ['.txt', '.csv']:
            # Pattern now specifically matches the expected symbol
            pattern = re.compile(rf"^{expected_symbol}\.Circulation_Patron_Report_Full\.(\d{{8}})\{ext}$")
            for file_path in downloads_dir.glob(f"{expected_symbol}.Circulation_Patron_Report_Full.*{ext}"):
                m = pattern.match(file_path.name)
                if m:
                    date_str = m.group(1)
                    try:
                        file_date = datetime.strptime(date_str, "%Y%m%d")
                        patron_files.append((file_path, file_date, expected_symbol))
                    except ValueError:
                        continue

        if not patron_files:
            raise FileNotFoundError(f"No patron files found for {expected_symbol} in downloads directory")
        
        # Get the most recent file for this library
        txt_local, _, symbol = max(patron_files, key=lambda x: x[1])
        logger.info(f"Using existing file: {txt_local}")

    else:
        # Connect SFTP
        user, pwd = get_credentials(args.lib_code)
        ssh, sftp = connect_sftp(user, pwd, verify=True)
        try:
            # First try the original pattern (likely .txt)
            files = list_remote_files(sftp, args.remote_dir)
            pattern = re.compile(args.pattern)
            latest_name, symbol = pick_latest_full_patron(files, pattern)
            txt_local = download_file(sftp, args.remote_dir, latest_name, downloads_dir)
        except (FileNotFoundError, ValueError) as e:
            logger.warning(f"Failed with original pattern: {e}")
            logger.info("Trying both .txt and .csv extensions...")
            # Try both extensions
            txt_local, symbol = download_patron_file_any_extension(
                sftp, args.remote_dir, get_flexible_pattern(), downloads_dir
            )
        finally:
            sftp.close()
        ssh.close()

    # Load TXT
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
        logger.warning("Incoming file does not include 'Patron_Barcode' column; cannot apply barcode updates.")

    # Apply filtering if requested
    skipped_df = None
    if args.filter_email_domain:
        logger.info("Email domain filtering enabled")
        
        # Validate that domains start with @
        valid_domains = []
        for domain in args.filter_email_domain:
            if not domain.startswith('@'):
                domain = '@' + domain
            valid_domains.append(domain)
        
        # Filter patrons
        in_df, skipped_df = filter_patrons_by_criteria(
            in_df,
            valid_domains=valid_domains
        )
        
        # Add matched_email column for later use in idAtSource
        if args.set_idsource_from_email:
            logger.info("Will use matched email as idAtSource")
            # matched_email column already added by filter function
        
        # Add source_system_value column if specified
        if args.source_system:
            logger.info(f"Will set sourceSystem to: {args.source_system}")
            in_df['source_system_value'] = args.source_system
        
        # Write skipped patrons report
        if skipped_df is not None and not skipped_df.empty:
            write_skipped_patrons_report(skipped_df, output_dir, symbol)
    
    # Load and validate updates (if present)
    updates_df = load_patron_updates(project_root)
    
    if updates_df is not None:
        # Only validate barcodes if patron_barcode_new column exists
        if "patron_barcode_new" in updates_df.columns:
            existing = set(in_df.get("Patron_Barcode", pd.Series([], dtype=str)).astype(str).str.strip())
            updates_df = preflight_validate_updates(
                updates_df,
                existing_barcodes_in_file=existing,
                soft_max_chars=args.soft_max_barcode_len,
                hard_max_bytes=args.hard_max_barcode_bytes,
            )
    else:
        logger.info("No patron_updates.txt or barcode_updates.txt found; will load all rows.")

    # Apply updates (or pass through)
    # Apply updates (or pass through)
    try:
        in_df_updated = apply_patron_updates_if_any(in_df, updates_df, sync_illid=args.sync_illid_to_barcode)
    except ValueError as e:
        # Error - wrong library's update file
        logger.error(str(e))
        sys.exit(1)

    # Determine symbol if we didn't get one from filename
    if not symbol:
        symbol = detect_symbol_from_txt(txt_local) or "UNK"

    # Get institution ID from environment variable
    institution_id = get_institution_id(args.lib_code)
    if institution_id is None:
        # Extract symbol for error message
        parts = args.lib_code.split('_')
        symbol = parts[-1].upper() if len(parts) >= 2 else args.lib_code.upper()
        
        logger.error(
            "Institution ID is REQUIRED for patron reload operations. "
            "Set %s_INSTITUTION_ID in .env file (e.g., %s_INSTITUTION_ID=12345)",
            symbol, symbol
        )
        sys.exit(1)

    logger.info(f"Using institution ID: {institution_id}")

    # Load headers and build formatted output
    headers = load_headers(headers_file)
    formatted_df = build_formatted_df(
        in_df_updated, 
        headers, 
        institution_id, 
        can_self_edit=(args.can_self_edit == "true"),
        use_expiration_date=args.use_expiration_date,
        use_source_value=args.use_source_value
    )

# Write output file
    processed_dir.mkdir(parents=True, exist_ok=True)
    out_name = f"{symbol}patronreload.txt"
    out_path = processed_dir / out_name
    formatted_df.to_csv(out_path, sep="\t", index=False, quoting=csv.QUOTE_MINIMAL)

    logger.info("Wrote reload file: %s (rows: %d)", out_path, len(formatted_df))
    
    # IMPORTANT: Remind user to verify
    if args.use_source_value:
        logger.warning("\n" + "="*60)
        logger.warning("IMPORTANT: VERIFY YOUR OUTPUT BEFORE UPLOADING")
        logger.warning("Check these fields in your output file:")
        logger.warning("  - idAtSource: Verify IDs from are current/correct")
        logger.warning("  - sourceSystem: Verify system identifier is correct")
        logger.warning("="*60 + "\n")

    # Optional upload
    if args.upload or args.upload_test:
        if args.upload_test:
            logger.info("Uploading reload file to TEST directory...")
            logger.warning("="*60)
            logger.warning("UPLOADING TO TEST ENVIRONMENT")
            logger.warning("Path: /xfer/wms/test/in/patron")
            logger.warning("="*60)
            remote_path = upload_patron_reload(args.lib_code, out_path, remote_dir="/xfer/wms/test/in/patron")
        else:
            logger.info("Uploading reload file to PRODUCTION...")
            logger.warning("="*60)
            logger.warning("UPLOADING TO PRODUCTION ENVIRONMENT")
            logger.warning("Path: /xfer/wms/in/patron")
            logger.warning("="*60)
            remote_path = upload_patron_reload(args.lib_code, out_path)
        
        logger.info("Uploaded successfully: %s", remote_path)

if __name__ == "__main__":
    main()