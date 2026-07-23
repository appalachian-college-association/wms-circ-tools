#!/usr/bin/env python3
"""
Email-domain filtering helpers for the patron reload builder.

Filters patrons by expiration status and email domain, chooses a unique matched
email per patron, and writes a report of any skipped records.
"""

import re
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)

# Default column names in the incoming OCLC patron file. Callers may override any
# subset via the ``columns`` argument to :func:`filter_patrons_by_criteria`.
DEFAULT_COLUMNS = {
    "barcode": "Patron_Barcode",
    "email": "Patron_Email_Address",
    "username": "Patron_Username",
    "expiration": "Patron_Expiration_Date",
}


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
            # Pattern: word chars, dots, hyphens before @ + domain
            pattern = r'[\w\.-]+' + re.escape(domain_lower)
            match = re.search(pattern, value_str)
            if match:
                return match.group(0)

    return None


def _skip_record(row: pd.Series, cols: dict, reason: str, email=None) -> dict:
    """Build a skipped-patron record dict for the report."""
    return {
        'barcode': row.get(cols["barcode"], ''),
        'familyName': row.get('Patron_Family_Name', ''),
        'givenName': row.get('Patron_Given_Name', ''),
        'email': row.get(cols["email"], '') if email is None else email,
        'skip_reason': reason,
    }


def _filter_expired(working_df: pd.DataFrame, cols: dict, skipped_records: list) -> pd.DataFrame:
    """Remove patrons whose expiration date is before today; record them as skipped."""
    expiration_col = cols["expiration"]
    if expiration_col not in working_df.columns:
        logger.warning("Column '%s' not found - skipping expiration filter", expiration_col)
        return working_df

    # Parse expiration dates
    working_df[expiration_col] = pd.to_datetime(
        working_df[expiration_col],
        errors="coerce",
    ).dt.normalize()

    today = pd.Timestamp.today().normalize()

    # Find expired patrons
    expired_mask = working_df[expiration_col] < today

    if expired_mask.any():
        # Record skipped expired patrons
        for _, row in working_df[expired_mask].iterrows():
            skipped_records.append(
                _skip_record(row, cols, f"Expired: {row.get(expiration_col, 'N/A')}")
            )

        logger.info("Filtered out %s expired patrons", f"{expired_mask.sum():,}")
        working_df = working_df[~expired_mask].copy()

    return working_df


def _match_valid_emails(
    working_df: pd.DataFrame, valid_domains: list, cols: dict, skipped_records: list
) -> pd.DataFrame:
    """Attach a 'matched_email' column and keep only patrons with a valid email."""
    email_col = cols["email"]
    username_col = cols["username"]

    working_df['matched_email'] = None

    for idx, row in working_df.iterrows():
        matched_email = None

        # First try primary email column
        if email_col in working_df.columns:
            matched_email = extract_email_from_field(row.get(email_col, ''), valid_domains)

        # If no match, try username column
        if not matched_email and username_col in working_df.columns:
            matched_email = extract_email_from_field(row.get(username_col, ''), valid_domains)

        # Store the matched email (or None)
        working_df.at[idx, 'matched_email'] = matched_email

        # Track if no match found
        if not matched_email:
            skipped_records.append(_skip_record(row, cols, 'No valid email domain found'))

    has_email_mask = working_df['matched_email'].notna()

    if not has_email_mask.any():
        logger.error("No patrons matched the email domain criteria!")
        logger.error("Checked domains: %s", ", ".join(valid_domains))
        raise ValueError(f"No patrons found with email domains: {', '.join(valid_domains)}")

    logger.info("Matched valid email for %s patrons", f"{has_email_mask.sum():,}")
    logger.info("Filtered out %s patrons without valid email", f"{(~has_email_mask).sum():,}")

    return working_df[has_email_mask].copy()


def _drop_duplicate_emails(
    working_df: pd.DataFrame, cols: dict, skipped_records: list
) -> pd.DataFrame:
    """Remove patrons sharing a matched email with another patron; record them as skipped."""
    logger.info("Checking for duplicate/shared email addresses...")

    # Count occurrences of each email
    email_counts = working_df['matched_email'].value_counts()
    duplicate_emails = email_counts[email_counts > 1]

    if duplicate_emails.empty:
        return working_df

    logger.warning("=" * 60)
    logger.warning("DUPLICATE/SHARED EMAIL ADDRESSES FOUND")
    logger.warning("=" * 60)
    logger.warning("Found %d email addresses used by multiple patrons:", len(duplicate_emails))

    for email, count in duplicate_emails.items():
        logger.warning("  %s: %s patrons", email, count)

    logger.warning("These patrons will be SKIPPED (cannot use shared email as unique idAtSource)")
    logger.warning("=" * 60)

    # Filter out patrons with duplicate emails
    duplicate_mask = working_df['matched_email'].isin(duplicate_emails.index)

    # Record skipped patrons with duplicate emails
    for _, row in working_df[duplicate_mask].iterrows():
        email = row['matched_email']
        count = email_counts[email]
        skipped_records.append(
            _skip_record(
                row, cols, f"Shared/duplicate email (used by {count} patrons)", email=email
            )
        )

    logger.info("Filtered out %s patrons with shared emails", f"{duplicate_mask.sum():,}")
    return working_df[~duplicate_mask].copy()


def filter_patrons_by_criteria(
    df: pd.DataFrame,
    valid_domains: list,
    columns: Optional[dict] = None,
    check_uniqueness: bool = True,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Filter patrons by expiration status and email domain.

    Filters applied in order:
    1. Remove expired patrons (expiration date < today)
    2. Check email column for valid domain, fallback to username column
    3. Remove patrons without valid email match
    4. Remove patrons with non-unique emails (if check_uniqueness=True)

    Args:
        df: Input patron DataFrame
        valid_domains: List of valid email domains to match
        columns: Optional overrides for column names (keys: barcode, email,
            username, expiration); defaults come from DEFAULT_COLUMNS
        check_uniqueness: If True, filter out patrons with duplicate emails

    Returns:
        Tuple of (filtered_df, skipped_df)
        - filtered_df: Patrons that passed all filters with new 'matched_email' column
        - skipped_df: Patrons that were filtered out with 'skip_reason' column
    """
    cols = {**DEFAULT_COLUMNS, **(columns or {})}

    logger.info("=" * 60)
    logger.info("Filtering patrons by criteria")
    logger.info("=" * 60)
    logger.info("Starting with %s total patrons", f"{len(df):,}")
    logger.info("Valid email domains: %s", ", ".join(valid_domains))

    # Track skipped records
    skipped_records = []

    # Make a copy to avoid modifying original
    working_df = df.copy()

    # Step 1: Filter out expired patrons
    working_df = _filter_expired(working_df, cols, skipped_records)
    logger.info("After expiration filter: %s patrons", f"{len(working_df):,}")

    # Steps 2-3: Match a valid email per patron, keep only those with a match
    working_df = _match_valid_emails(working_df, valid_domains, cols, skipped_records)

    # Step 4: Check for duplicate emails (generic/shared addresses)
    if check_uniqueness:
        working_df = _drop_duplicate_emails(working_df, cols, skipped_records)

    # Create skipped DataFrame
    skipped_df = pd.DataFrame(skipped_records)

    logger.info("=" * 60)
    logger.info("Final filtered count: %s patrons", f"{len(working_df):,}")
    logger.info("Total skipped: %s patrons", f"{len(skipped_df):,}")
    logger.info("=" * 60)

    return working_df, skipped_df


def write_skipped_patrons_report(
    skipped_df: pd.DataFrame, output_dir: Path, symbol: str
) -> Optional[Path]:
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

    logger.info("Skipped patrons report written: %s", report_path)
    logger.info("  - Total skipped: %s", f"{len(skipped_df):,}")

    # Log breakdown by reason
    if 'skip_reason' in skipped_df.columns:
        reason_counts = skipped_df['skip_reason'].value_counts()
        logger.info("  - Breakdown by reason:")
        for reason, count in reason_counts.items():
            logger.info("      %s: %s", reason, f"{count:,}")

    return report_path
