#!/usr/bin/env python3
"""
exception_report.py
-------------------
Parse OCLC patron-load exception reports.

Used by: build_patron_updates.py, idm_blank_patron_tool.py

HOW AN EXCEPTION FILE IS STRUCTURED
Each failure is two lines. The first names the error, e.g.

    Error occurred while creating a new user... COMPLETE_CREATE_FAILURE :
        new username is already used ...
    Error occurred while updating user... FAILURE... USERDATA_UPDATE_FAILURE...
        DUPLICATE_BARCODE_ERROR

The second is the rejected record echoed back as a tab-delimited row in the
46-column reload layout (the same column order as headers_formattedpatron.txt),
so 'barcode', 'idAtSource', 'sourceSystem', 'givenName', 'familyName',
'emailAddress' and 'username' can all be read from it by column name.

If a data row does not have the expected number of tab-separated fields we
fall back to taking its last whitespace token as the username (an email
address in practice) and leave the other fields empty.
"""

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from file_utils import load_headers

logger = logging.getLogger(__name__)

DEFAULT_HEADERS_FILE = Path("headers_formattedpatron.txt")

# Error-type labels we recognise in the header line. Anything else is "OTHER".
COMPLETE_CREATE_FAILURE = "COMPLETE_CREATE_FAILURE"
DUPLICATE_BARCODE_ERROR = "DUPLICATE_BARCODE_ERROR"
OTHER = "OTHER"
KNOWN_ERROR_TYPES = (COMPLETE_CREATE_FAILURE, DUPLICATE_BARCODE_ERROR)

_HEADER_LINE = re.compile(r"^\s*Error occurred", re.IGNORECASE)


@dataclass
class Failure:
    """One failed record from an exception report."""
    line_no: int            # 1-based line number of the "Error occurred" line
    error_type: str         # COMPLETE_CREATE_FAILURE, DUPLICATE_BARCODE_ERROR or OTHER
    error_text: str         # the full header line
    fields: dict = field(default_factory=dict)   # reload column -> value ({} if row unreadable)
    username: str = ""      # fields["username"] or the row's last whitespace token

    def get(self, column: str, default: str = "") -> str:
        """Return a stripped field value by reload column name."""
        return str(self.fields.get(column, default)).strip()


# Other wordings OCLC uses for the same underlying problem
_ERROR_ALIASES = {
    "BARCODE IS ALREADY USED": DUPLICATE_BARCODE_ERROR,
}


def classify_error(header_line: str) -> str:
    """Map an 'Error occurred ...' line to one of the known error types."""
    upper = header_line.upper()
    for label in KNOWN_ERROR_TYPES:
        if label in upper:
            return label
    for phrase, label in _ERROR_ALIASES.items():
        if phrase in upper:
            return label
    return OTHER


def load_column_index(headers_file: Path = DEFAULT_HEADERS_FILE) -> Optional[dict]:
    """
    Map reload column name -> 0-based position from headers_formattedpatron.txt.
    Returns None (with a warning) if the file is missing or lacks 'username'.
    """
    if not headers_file.exists():
        logger.warning(
            "%s not found; exception rows will be read by last token only", headers_file
        )
        return None
    index = {name: pos for pos, name in enumerate(load_headers(headers_file))}
    if "username" not in index:
        logger.warning("%s has no 'username' column; rows read by last token only", headers_file)
        return None
    return index


def parse_data_line(data_line: str, col_index: Optional[dict]) -> tuple:
    """
    Split one echoed record into (fields, username).

    fields is a dict keyed by reload column name when the row has exactly as
    many tab-separated values as the headers file; otherwise {}.
    """
    values = data_line.split("\t")
    if col_index is not None and len(values) == len(col_index):
        fields = {name: values[pos].strip() for name, pos in col_index.items()}
        return fields, fields.get("username", "")

    tokens = [t for t in re.split(r"\s+", data_line.strip()) if t]
    username = tokens[-1] if tokens else ""
    if username:
        logger.warning(
            "Data row has %d tab fields (expected %s); using last token '%s' as username",
            len(values), len(col_index) if col_index else "46", username,
        )
    return {}, username


def parse_exception_report(path: Path, headers_file: Path = DEFAULT_HEADERS_FILE) -> list:
    """Read an exception report and return a list of Failure objects (may be empty)."""
    col_index = load_column_index(headers_file)
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    failures = []

    i = 0
    while i < len(lines):
        line = lines[i]
        if not _HEADER_LINE.match(line):
            i += 1
            continue

        # The next non-blank line is the echoed record
        j = i + 1
        while j < len(lines) and not lines[j].strip():
            j += 1

        error_type = classify_error(line)
        if j >= len(lines) or _HEADER_LINE.match(lines[j]):
            logger.warning("%s at line %d has no data row", error_type, i + 1)
            i = j
            continue

        fields, username = parse_data_line(lines[j], col_index)
        if not username and not fields:
            logger.warning("%s at line %d: data row could not be read", error_type, i + 1)
        else:
            failures.append(Failure(i + 1, error_type, line.strip(), fields, username))
        i = j + 1

    counts = count_by_type(failures)
    logger.info(
        "Parsed %d failure(s) from %s: %s", len(failures), path.name,
        ", ".join(f"{k}={v}" for k, v in counts.items()) or "none",
    )
    return failures


def count_by_type(failures: list) -> dict:
    """Return {error_type: count} in a stable order."""
    counts = {}
    for f in failures:
        counts[f.error_type] = counts.get(f.error_type, 0) + 1
    return counts
