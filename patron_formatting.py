#!/usr/bin/env python3
"""
Formatting and update helpers for the patron reload builder.

Maps the incoming OCLC patron report columns to the 46-column reload layout,
optionally cleaning pipe-delimited source fields, and applies field updates from
patron_updates.txt.
"""

import os
import logging
from collections import Counter
from dataclasses import dataclass
from typing import Optional

import pandas as pd

from file_utils import extract_first_part_from_pipe_delimited, analyze_pipe_delimited_patterns

logger = logging.getLogger(__name__)


INCOMING_TO_FORMATTED_MAP = {
    "prefix": "prefix",
    "givenName": "Patron_Given_Name",
    "middleName": "middleName",
    "familyName": "Patron_Family_Name",
    "suffix": "suffix",
    "nickname": "nickname",
    # Excluded columns load as blank ""
    # Optional: add mappings here (final output column:change "" to column in incoming OCLC TXT)
    # Optional: if mapped here, can add to field_mappings
    # (patron_updates.txt column:incoming OCLC TXT column)
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

# Update-file column -> OCLC data column (used when applying patron_updates.txt)
FIELD_MAPPINGS = {
    "familyName": "Patron_Family_Name",
    "givenName": "Patron_Given_Name",
    "borrowerCategory": "Patron_Borrower_Category",
    "homeBranch": "Patron_Home_Branch_ID",
    "emailAddress": "Patron_Email_Address",
    "username": "Patron_Username",
    "illId": "illId",
    "idAtSource": "Patron_User_ID_At_Source",
    "sourceSystem": "Patron_Source_System",
    # Add other fields as needed
}


@dataclass
class FormatOptions:
    """Flags controlling how the reload file is built."""
    can_self_edit: bool = False
    use_expiration_date: bool = False
    use_source_value: bool = False


def get_expiration_date() -> Optional[str]:
    """Get EXPIRATION_DATE from .env file if set and not 'IGNORE'."""
    exp_date = os.getenv("EXPIRATION_DATE", "").strip()
    if not exp_date or exp_date.upper() == "IGNORE":
        return None
    return exp_date


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

    logger.info(
        "Most common sourceSystem: %s (appears %d times)",
        most_common_system, system_counts[most_common_system],
    )
    return most_common_system


def process_special_fields(in_df: pd.DataFrame, skip_columns: list = None) -> pd.DataFrame:
    """
    Process idAtSource and sourceSystem fields to extract clean values.
    Handles pipe-delimited junk data by taking the first part.

    WARNING: If values have been updated over time, old values may appear
    first in the pipe-delimited list. REVIEW the log output and verify your
    output file before uploading!

    Args:
        in_df: Input DataFrame
        skip_columns: Column names to skip processing (already updated via patron_updates.txt)
    """
    if skip_columns is None:
        skip_columns = []

    df = in_df.copy()

    # Process idAtSource
    if "Patron_User_ID_At_Source" in df.columns and "Patron_User_ID_At_Source" not in skip_columns:
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
            logger.info("Sample extracted idAtSource values: %s", non_empty.head(3).tolist())
    elif "Patron_User_ID_At_Source" in skip_columns:
        logger.info("Skipping idAtSource processing - using values from patron_updates.txt")

    # Process sourceSystem
    if "Patron_Source_System" in df.columns and "Patron_Source_System" not in skip_columns:
        logger.info("Processing sourceSystem field...")

        # Analyze the data first
        analyze_pipe_delimited_patterns(df, "Patron_Source_System")

        # Find most common first-part value
        most_common_value = find_most_common_source_system(df, "Patron_Source_System")
        if most_common_value:
            df["Patron_Source_System"] = most_common_value
            logger.info("Set all sourceSystem values to: %s", most_common_value)
        else:
            logger.warning("No valid sourceSystem value found, leaving column empty")
            df["Patron_Source_System"] = ""
    elif "Patron_Source_System" in skip_columns:
        logger.info("Skipping sourceSystem processing - using values from patron_updates.txt")

    return df


def _detect_skip_processing(in_df: pd.DataFrame) -> list:
    """Return the OCLC columns whose values came from patron_updates.txt and must be preserved."""
    skip_processing = []
    if "idAtSource" in in_df.columns and (in_df["idAtSource"] != "").any():
        skip_processing.append("Patron_User_ID_At_Source")
        logger.info("Detected idAtSource updates - will preserve patron_updates.txt values")

    if "sourceSystem" in in_df.columns and (in_df["sourceSystem"] != "").any():
        skip_processing.append("Patron_Source_System")
        logger.info("Detected sourceSystem updates - will preserve patron_updates.txt values")

    return skip_processing


def _assign_source_field(out: pd.DataFrame, processed_df: pd.DataFrame, formatted_col: str) -> None:
    """
    Assign idAtSource/sourceSystem using this priority order:
      1) Values from patron_updates.txt (columns named idAtSource/sourceSystem)
      2) Values from filtering (matched_email, source_system_value)
      3) --use-source-value flag (processed OCLC Patron_User_ID_At_Source/Patron_Source_System)
      4) Empty string
    """
    if formatted_col in processed_df.columns and (processed_df[formatted_col] != "").any():
        # Use the value from patron_updates.txt (preserves updates)
        out[formatted_col] = processed_df[formatted_col].astype(str).fillna("")
        logger.info("Using %s values from patron_updates.txt", formatted_col)
    elif formatted_col == "idAtSource" and "matched_email" in processed_df.columns:
        # Use matched email as idAtSource if available (from filtering)
        out[formatted_col] = processed_df["matched_email"].astype(str).fillna("")
    elif formatted_col == "sourceSystem" and "source_system_value" in processed_df.columns:
        # Use specified source system value if available (from filtering)
        out[formatted_col] = processed_df["source_system_value"].astype(str).fillna("")
    elif formatted_col == "idAtSource" and "Patron_User_ID_At_Source" in processed_df.columns:
        # Fall back to processed OCLC value
        out[formatted_col] = processed_df["Patron_User_ID_At_Source"].astype(str).fillna("")
    elif formatted_col == "sourceSystem" and "Patron_Source_System" in processed_df.columns:
        # Fall back to processed OCLC value
        out[formatted_col] = processed_df["Patron_Source_System"].astype(str).fillna("")
    else:
        out[formatted_col] = ""


def _apply_canselfedit(out: pd.DataFrame, processed_df: pd.DataFrame, can_self_edit: bool) -> None:
    """Set canSelfEdit from patron_updates.txt where present, else the default."""
    if "canSelfEdit" in processed_df.columns:
        # Use values from updates file where available, default for others
        out["canSelfEdit"] = processed_df["canSelfEdit"].replace(
            "", "true" if can_self_edit else "false"
        )
        updated_count = (processed_df["canSelfEdit"] != "").sum()
        if updated_count > 0:
            logger.info(
                "Using canSelfEdit values from patron_updates.txt for %d rows", updated_count
            )
    else:
        # No updates file or no canSelfEdit column - use default
        out["canSelfEdit"] = "true" if can_self_edit else "false"


def _apply_illid(out: pd.DataFrame, processed_df: pd.DataFrame) -> None:
    """Carry synced illId values through, else leave the (excluded) column empty."""
    if "illId" in processed_df.columns:
        out["illId"] = processed_df["illId"].astype(str).fillna("")
        synced_count = (processed_df["illId"] != "").sum()
        if synced_count > 0:
            logger.info("Using synced illId values for %d rows", synced_count)
    else:
        # illId wasn't synced, leave as excluded (empty)
        out["illId"] = ""


def _apply_expiration(out: pd.DataFrame, use_expiration_date: bool) -> None:
    """Set oclcExpirationDate from EXPIRATION_DATE when requested, else leave empty."""
    if use_expiration_date:
        exp_date = get_expiration_date()
        if exp_date:
            logger.info("Setting oclcExpirationDate to: %s", exp_date)
            out["oclcExpirationDate"] = exp_date
        else:
            logger.info(
                "EXPIRATION_DATE not found or set to IGNORE, leaving oclcExpirationDate empty"
            )
            out["oclcExpirationDate"] = ""
    else:
        out["oclcExpirationDate"] = ""


def build_formatted_df(
    in_df: pd.DataFrame,
    headers: list,
    institution_id: str,
    options: FormatOptions,
) -> pd.DataFrame:
    """Create a DataFrame with 46 columns in the exact order from headers."""
    # Determine which columns were updated via patron_updates.txt
    # These should NOT be processed by process_special_fields
    skip_processing = _detect_skip_processing(in_df)

    # Process special fields that need cleaning (but skip columns that were updated)
    if options.use_source_value:
        logger.info("Processing source fields (--use-source-value enabled)")
        processed_df = process_special_fields(in_df, skip_columns=skip_processing)
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
            _assign_source_field(out, processed_df, formatted_col)
        elif incoming_col == "":
            # This field is excluded - leave as empty string
            out[formatted_col] = ""
        elif incoming_col in processed_df.columns:
            # Map the processed data
            out[formatted_col] = processed_df[incoming_col].astype(str).fillna("")
        else:
            # Column doesn't exist in input, leave empty
            out[formatted_col] = ""

    # Special-cases / computed fields
    _apply_canselfedit(out, processed_df, options.can_self_edit)
    out["institutionId"] = str(institution_id) if institution_id is not None else ""
    _apply_illid(out, processed_df)
    _apply_expiration(out, options.use_expiration_date)

    # Ensure all columns are strings, preserve leading zeros
    for c in out.columns:
        out[c] = out[c].astype(str).fillna("")

    return out


def _apply_barcode_update(merged: pd.DataFrame, updates_df: pd.DataFrame, sync_illid: bool) -> None:
    """Apply patron_barcode_new to Patron_Barcode, optionally syncing illId to it."""
    if "patron_barcode_new" not in updates_df.columns:
        return

    merged["Patron_Barcode"] = merged["patron_barcode_new"].astype(str)
    logger.info("Updated Patron_Barcode for all matched rows")

    if sync_illid:
        if "illId" not in merged.columns:
            merged["illId"] = ""
        merged["illId"] = merged["Patron_Barcode"]
        logger.info("Synced illId to updated barcode")


def _apply_field_mappings(merged: pd.DataFrame, updates_df: pd.DataFrame) -> None:
    """Update mapped fields from the update file, only where the update value is non-blank."""
    for update_col, patron_col in FIELD_MAPPINGS.items():
        if update_col in updates_df.columns and patron_col in merged.columns:
            # Only update rows where the update value is non-blank
            update_mask = merged[update_col].astype(str).str.strip() != ""
            if update_mask.any():
                merged.loc[update_mask, patron_col] = (
                    merged.loc[update_mask, update_col].astype(str)
                )
                updated_count = update_mask.sum()
                logger.info(
                    "Updated %s for %d rows (preserved OCLC values where update was blank)",
                    patron_col, updated_count,
                )
            else:
                logger.info(
                    "No non-blank updates found for %s, preserving all OCLC values", patron_col
                )


def _apply_canselfedit_update(merged: pd.DataFrame, updates_df: pd.DataFrame) -> None:
    """Normalize/validate canSelfEdit from the update file so build_formatted_df can use it."""
    if "canSelfEdit" not in updates_df.columns:
        return

    # Normalize to lowercase true/false for consistency
    merged["canSelfEdit"] = merged["canSelfEdit"].astype(str).str.lower()
    # Ensure only valid values
    valid_mask = merged["canSelfEdit"].isin(["true", "false"])
    if not valid_mask.all():
        invalid_count = (~valid_mask).sum()
        logger.warning(
            "%d canSelfEdit values are not 'true' or 'false', will use default", invalid_count
        )
        merged.loc[~valid_mask, "canSelfEdit"] = ""
    logger.info("Set canSelfEdit from patron_updates.txt for matched rows")


def _select_output_columns(merged: pd.DataFrame, in_df: pd.DataFrame) -> pd.DataFrame:
    """Keep the original incoming columns plus special columns that flow through."""
    # Keep only original columns (drop the _upd and update-file columns)
    keep_cols = [c for c in merged.columns if c in in_df.columns]

    # Preserve special columns that aren't in the incoming file but need to flow through
    special_cols = ["canSelfEdit", "illId", "idAtSource", "sourceSystem"]
    for col in special_cols:
        if col in merged.columns and col not in in_df.columns:
            keep_cols.append(col)

    return merged[keep_cols]


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

    logger.info("Matched %d patron rows to update", len(merged))

    # Apply field updates - only update if non-blank (barcode is a special case)
    _apply_barcode_update(merged, updates_df, sync_illid)
    _apply_field_mappings(merged, updates_df)

    # canSelfEdit doesn't exist in incoming TXT but must flow through to build_formatted_df
    _apply_canselfedit_update(merged, updates_df)

    return _select_output_columns(merged, in_df)
