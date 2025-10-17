# file_utils.py
"""
File reading and parsing utilities for OCLC WMS Circulation Tools
------------------------------------------------------------------
This module provides functions for reading and parsing various file formats
used in patron and circulation workflows.

Used by: circ_patron_reload.py, delete_expired_patrons.py
"""

import logging
from pathlib import Path
import pandas as pd

logger = logging.getLogger(__name__)


def safe_read_txt(path: Path) -> pd.DataFrame:
    """
    Read delimited file (CSV, TSV, or pipe-delimited) with auto-detection.
    
    Args:
        path: Path to file to read
        
    Returns:
        DataFrame with all columns as strings
        
    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If file is empty
    """
    logger.info(f"Attempting to read file: {path}")
    
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    
    file_size = path.stat().st_size
    logger.info(f"File size: {file_size} bytes")
    
    if file_size == 0:
        raise ValueError(f"File is empty: {path}")
    
    # Detect delimiter from first line
    with open(path, 'r', encoding='utf-8') as f:
        first_line = f.readline().strip()
    
    pipe_count = first_line.count('|')
    tab_count = first_line.count('\t')
    comma_count = first_line.count(',')
    
    # Determine delimiter (highest count wins)
    delimiter_counts = [
        (pipe_count, '|', 'pipe'),
        (tab_count, '\t', 'tab'),
        (comma_count, ',', 'comma')
    ]
    delimiter_counts.sort(reverse=True, key=lambda x: x[0])
    
    detected_delimiter = delimiter_counts[0][1]
    delimiter_name = delimiter_counts[0][2]
    
    logger.info(f"Detected delimiter: {delimiter_name} (count: {delimiter_counts[0][0]})")
    
    # Read with detected delimiter
    try:
        df = pd.read_csv(path, sep=detected_delimiter, dtype=str).fillna("")
        logger.info(f"Successfully read file with {len(df)} rows and {len(df.columns)} columns")
        return df
    except Exception as e:
        logger.error(f"Error reading with {delimiter_name} delimiter: {e}")
        # Fallback to pandas auto-detection
        logger.info("Trying pandas auto-detection...")
        df = pd.read_csv(path, sep=None, engine='python', dtype=str).fillna("")
        logger.info(f"Success with auto-detection: {len(df)} rows, {len(df.columns)} columns")
        return df


def load_headers(headers_file: Path) -> list[str]:
    """
    Load tab-delimited headers from file.
    
    Args:
        headers_file: Path to headers file
        
    Returns:
        List of header column names
    """
    txt = headers_file.read_text(encoding="utf-8").strip()
    return [h.strip() for h in txt.split("\t") if h.strip()]

def extract_first_part_from_pipe_delimited(value: str, field_name: str = "value") -> str:
    """
    Extract the first part from a pipe-delimited string.
    
    WARNING: If your source system has been updated and OCLC appended new 
    values instead of replacing old ones, this function takes the FIRST part, 
    which may be outdated. Review your output before uploading!
    
    Args:
        value: String that may contain pipe-delimited parts
        field_name: Name of field being processed (for logging)
        
    Returns:
        First part before pipe, or entire value if no pipe exists
        
    Examples:
        "a77d8ccd-a0da|br03312" → "a77d8ccd-a0da"
        "urn:mace:oclc:idm:lib|update" → "urn:mace:oclc:idm:lib"
        "user@example.com" → "user@example.com"
    """
    if pd.isna(value) or value == "":
        return ""
    
    value_str = str(value).strip()
    
    # If no pipe delimiter, return as-is
    if '|' not in value_str:
        return value_str
    
    # Split by pipe
    parts = [part.strip() for part in value_str.split('|')]
    
    # Log when we're discarding data
    if len(parts) > 1:
        logger.debug(f"{field_name}: Found {len(parts)} parts, using first: '{parts[0]}' (discarding: {parts[1:]})")
    
    return parts[0] if parts and parts[0] else ""

def extract_last_part_from_pipe_delimited(value: str, field_name: str = "value") -> str:
    """
    Extract the last part from a pipe-delimited string.
    
    WARNING: If your source system has been updated and OCLC appended new 
    values instead of replacing old ones, this function takes the LAST part, 
    which may be the most recent. Review your output before uploading!
    
    Args:
        value: String that may contain pipe-delimited parts
        field_name: Name of field being processed (for logging)
        
    Returns:
        Value after final pipe, or entire value if no pipe exists
        
    Examples:
        "a77d8ccd-a0da|br03312" → "br03312"
        "urn:mace:oclc:idm:lib|update" → "update"
        "user@example.com" → "user@example.com"
    """
    if pd.isna(value) or value == "":
        return ""
    
    value_str = str(value).strip()
    
    # If no pipe delimiter, return as-is
    if '|' not in value_str:
        return value_str
    
    # Split by pipe
    parts = [part.strip() for part in value_str.split('|')]
    
    # Log when we're discarding data
    if len(parts) > 1:
        logger.debug(f"{field_name}: Found {len(parts)} parts, using last: '{parts[-1]}' (discarding: {parts[:-1]})")
    
    return parts[-1] if parts and parts[-1] else ""

def analyze_pipe_delimited_patterns(df: pd.DataFrame, column_name: str) -> None:
    """
    Analyze pipe-delimited values and warn about potential issues.
    
    This helps users identify if their data has been updated and old values
    are appearing first (which would be incorrectly extracted).
    """
    if column_name not in df.columns:
        return
    
    values_with_pipes = df[column_name][df[column_name].astype(str).str.contains('|', na=False)]
    
    if values_with_pipes.empty:
        logger.info(f"{column_name}: No pipe-delimited values found (clean data)")
        return
    
    pipe_count = len(values_with_pipes)
    total_count = len(df)
    percentage = (pipe_count / total_count) * 100
    
    logger.warning("="*60)
    logger.warning(f"{column_name}: Found {pipe_count} records ({percentage:.1f}%) with multiple pipe-delimited values")
    logger.warning("Script will extract FIRST part only")
    logger.warning("REVIEW OUTPUT before uploading to ensure correct values are used!")
    logger.warning("="*60)
    
    # Show sample of what's being extracted
    sample_values = values_with_pipes.head(5)
    logger.info(f"\nSample {column_name} values (showing first 5):")
    for idx, val in enumerate(sample_values, 1):
        parts = str(val).split('|')
        logger.info(f"  {idx}. Full value: {val}")
        logger.info(f"     Will extract: '{parts[0]}'")
        logger.info(f"     Will discard: {parts[1:]}")
    
    logger.info("")