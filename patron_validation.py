#!/usr/bin/env python3
"""
Validation helpers for the patron reload builder.

Loads patron_updates.txt (barcode/email/other field updates) and preflight-validates
"new" barcodes against OCLC/WMS restrictions before they are applied.
"""

import logging
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

RESERVED_URL_CHARS = set("!*'();:@&=+$,/?%#[]")


def utf8_len_bytes(s: str) -> int:
    """Return the length of a string in UTF-8 encoded bytes."""
    return len(s.encode("utf-8"))


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
        logger.info(
            "Loading patron updates from barcode_updates.txt "
            "(consider renaming to patron_updates.txt)"
        )
    else:
        return None

    df = pd.read_csv(p, sep="\t", dtype=str).fillna("")

    # patron_barcode_old is required to identify records
    if "patron_barcode_old" not in df.columns:
        raise ValueError(
            f"{p.name} must contain 'patron_barcode_old' column to identify patrons"
        )

    # Trim whitespace on all columns
    for col in df.columns:
        before = df[col].astype(str)
        after = before.str.strip()
        trimmed = (before != after).sum()
        if trimmed:
            logger.info(
                "Trimmed whitespace on %d '%s' values from %s", trimmed, col, p.name
            )
        df[col] = after

    # Log what updates will be applied
    update_cols = [c for c in df.columns if c != "patron_barcode_old"]
    logger.info(
        "Patron updates file contains %d rows with updates for: %s",
        len(df), ", ".join(update_cols),
    )

    return df


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
        logger.warning(
            "barcode_updates: %d rows have EMPTY patron_barcode_new; "
            "they will be ignored.", n
        )
        df = df.loc[~empty_new].copy()

    # Soft and hard limits
    too_long_soft = df["patron_barcode_new"].str.len() > soft_max_chars
    if too_long_soft.any():
        logger.warning(
            "barcode_updates: %d 'new' barcodes exceed soft max of %d characters "
            "(recommend shortening).",
            int(too_long_soft.sum()), soft_max_chars,
        )

    too_long_hard = df["patron_barcode_new"].map(utf8_len_bytes) > hard_max_bytes
    if too_long_hard.any():
        logger.warning(
            "barcode_updates: %d 'new' barcodes exceed %d BYTES "
            "(WMS barcode limit guidance).",
            int(too_long_hard.sum()), hard_max_bytes,
        )

    # Duplicates within the change list
    dupe_mask = df["patron_barcode_new"].duplicated(keep=False)
    if dupe_mask.any():
        sample = sorted(df.loc[dupe_mask, "patron_barcode_new"].unique())[:10]
        logger.warning(
            "barcode_updates: duplicate 'new' barcode values detected (sample): %s",
            ", ".join(sample),
        )

    # New barcode already exists in incoming file (possible collision)
    if existing_barcodes_in_file:
        olds = set(df["patron_barcode_old"])
        collisions = sorted(
            b for b in df["patron_barcode_new"]
            if b and (b in existing_barcodes_in_file and b not in olds)
        )
        if collisions:
            logger.warning(
                "barcode_updates: %d 'new' barcodes already exist in the incoming "
                "patron file (sample): %s",
                len(collisions), ", ".join(collisions[:10]),
            )

    # Reserved URL characters
    has_reserved = df["patron_barcode_new"].map(
        lambda s: any(ch in RESERVED_URL_CHARS for ch in s)
    )
    if has_reserved.any():
        sample = df.loc[has_reserved, "patron_barcode_new"].head(10).tolist()
        logger.warning(
            "barcode_updates: %d 'new' barcodes include reserved URL characters "
            "(sample): %s",
            int(has_reserved.sum()), ", ".join(sample),
        )

    return df
