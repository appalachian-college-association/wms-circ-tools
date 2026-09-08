# OCLC WMS Circulation Tools

Python tools for downloading, processing, and managing OCLC WorldShare Management Services (WMS) circulation data.

## Overview

This repository provides utilities to:
- **Download** circulation reports and patron/item data from OCLC SFTP
- **Process** patron records for reload operations (updates, barcode changes)
- **Generate** patron delete files for expired accounts
- **Manage** patron data with validation and safety checks
- **Find & remove** blank-name "ghost" patron records via the OCLC IDM API

These tools are designed for library staff working with WMS circulation data, with varying levels of Python experience. Detailed documentation and examples are provided for each tool.

## What's Included

### Patron Management Tools
- **`data_fetcher.py`** - Download circulation reports and patron files from OCLC SFTP
- **`data_fetcher_openrefine.py`** - Download circulation reports and patron files from OCLC SFTP with formatting for openrefine loading
- **`circ_patron_reload.py`** - Build patron reload files with optional updates (barcodes, email, etc.)
- **`build_patron_updates.py`** - Build `patron_updates.txt` from an OCLC upload exception report (COMPLETE_CREATE_FAILURE / username already used)
- **`check_source.py`** - Find patrons missing campus idAtSource/sourceSystem values; build `patron_updates.txt` for them and a review file for the rest
- **`delete_expired_patrons.py`** - Generate delete files for expired patron accounts
- **`idm_blank_patron_tool.py`** - Review/delete blank-name "ghost" patron records via the OCLC IDM (SCIM) API (not sFTP)

### Helper Modules
- **`sftp_utils.py`** - Shared SFTP connection and file transfer functions
- **`file_utils.py`** - File reading, parsing, and data cleaning utilities
- **`data_loader.py`** - Upload processed files back to OCLC sFTP
- **`exception_report.py`** - Parse OCLC load exception reports (error type + the echoed record's fields)

### Configuration Files
- **`headers_formattedpatron.txt`** - Column headers for patron reload files (46 fields)
- **`headers_deletes.txt`** - Column headers for patron delete files (5 fields)
- **`patron_updates.txt`** - User-supplied file for patron field updates (optional - use headers_formattedpatron.txt column names (except barcodes old/new) and verify/update mapping in circ_patron_reload.py)

📖 **Which file do I need?**
- **New here, or setting up a machine?** Follow the Quick Start and Common Workflows below.
- **Looking up a CLI flag, a field mapping, or an error message?** Go to [PATRON_TOOLS.md](PATRON_TOOLS.md) - it is the single reference for every script's options and troubleshooting.

## Quick Start

### Requirements
- **Python 3.9+**
- **Modules**: `paramiko`, `python-dotenv`, `pandas`, `requests`

Install dependencies:
```bash
pip install -r requirements.txt
```

### Initial Setup

**1. Clone this repository**
```bash
git clone https://github.com/yourusername/wms-circ-tools.git
cd wms-circ-tools
```

**2. Create your `.env` file**

Copy `sample.env` to `.env` and add your credentials:

```env
# OCLC SFTP Connection
HOST_NAME=sftp.oclc.org
HOST_PORT=22
FINGERPRINT=SHA256:your_verified_fingerprint_here

# Library Credentials (add one set per library)
WX_ABC_USER=your_oclc_username
WX_ABC_PASS=your_oclc_password

# Institution IDs (for delete operations)
ABC_INSTITUTION_ID=12345

# Optional: Expiration date for patron reloads
EXPIRATION_DATE=2026-12-31
```

**Note about naming**: Your credential variables must follow the pattern `{LIB_CODE}_USER` and `{LIB_CODE}_PASS` where LIB_CODE is your OCLC symbol. For example, if your library code is `wx_abc`, use `WX_ABC_USER` and `WX_ABC_PASS`.

**3. Verify OCLC host fingerprint**

For security, verify the SFTP server's identity on first connection:

```bash
python data_fetcher.py --print-fingerprint
```

Add the value to FINGERPRINT in your `.env` file. Required for patron tools.

**4. Test your connection**

Download the most recent patron file to verify setup:

```bash
python data_fetcher.py wx_abc --patrons --recent 1
```

## Common Workflows

### Download Reports
```bash
# Scrape entire directory (default, add CLI flags to specifiy downloads)
python data_fetcher.py wx_abc


# Download the most recent item file (default for --recent)
python data_fetcher.py wx_abc --recent

# Download patron files from specified date (YYYY-MM-DD)
python data_fetcher.py wx_abc --patrons --since 2025-09-01

# Download ten (10) latest available stats (patron load reports)
python data_fetcher.py wx_abc --stats --recent 10

# Download data formatted for OpenRefine import
python data_fetcher_openrefine.py wx_abc --recent 
```

### Update Patron Records
```bash
# Create reload file with updates from patron_updates.txt (no upload)
python circ_patron_reload.py wx_abc

# Same, using the newest already-downloaded patron report (no download, no upload)
python circ_patron_reload.py wx_abc --offline --use-source-value

# Same, using one specific (e.g., hand-edited) patron report
python circ_patron_reload.py wx_abc --input-file patrons/downloads/ABC_edited.txt --use-source-value

# Build and upload to OCLC in one step
python circ_patron_reload.py wx_abc --upload

# Upload a reload file you already built and reviewed, as-is (add --upload-test for the test directory)
python circ_patron_reload.py wx_abc --upload-file patrons/reloads/ABCpatronreload.txt
```
Flag details, file-search rules, and custom-file formatting: see [PATRON_TOOLS.md → circ_patron_reload.py](PATRON_TOOLS.md#circ_patron_reloadpy---patron-reloads). Logs are written to `logs/ABCpatronreload_MMDDYY.log`.

### Fix "username is already used" Load Failures
```bash
# Download the newest full patron report and load reports (incl. the .exception file) to reports/ABC/
python data_fetcher.py wx_abc --patrons --recent
python data_fetcher.py wx_abc --stats --recent 2

# Build patron_updates.txt for every COMPLETE_CREATE_FAILURE patron:
#   old barcode = OCLC's current barcode (patron report), new barcode = incoming barcode (exception row),
#   plus idAtSource + sourceSystem so OCLC can match the existing record
python build_patron_updates.py wx_abc --dry-run
python build_patron_updates.py wx_abc

# Reload ONLY those patrons, then review patrons/reloads/ABCpatronreload.txt before uploading
python circ_patron_reload.py wx_abc --offline --use-source-value
# Remove patron_updates.txt afterwards so it does not filter your next reload
```

### Add Campus Source Values (idAtSource / sourceSystem)
```bash
# Needs ABC_CAMPUS_DOMAIN and ABC_CAMPUS_SOURCE_SYSTEM in .env (see sample.env)
python data_fetcher.py wx_abc --patrons --recent

# Preview, then write patron_updates.txt (patrons missing the campus pair)
# and patrons/reports/ABC_source_review_YYYYMMDD.txt (patrons needing a campus email from the library)
python check_source.py wx_abc --dry-run
python check_source.py wx_abc

# Reload ONLY those patrons; review patrons/reloads/ABCpatronreload.txt before uploading
python circ_patron_reload.py wx_abc --offline
python circ_patron_reload.py wx_abc --upload-file patrons/reloads/ABCpatronreload.txt
```

### Fix DUPLICATE_BARCODE_ERROR Load Failures (ghost IDM records)
```bash
# IDM API key required in .env as ABC_IDM_CLIENT_ID and ABC_IDM_CLIENT_SECRET
# 1. Confirm each failed email resolves to one real record with the expected barcode, and get a worklist
#    (patrons/idm_review/ABC_ppid_needed_*.tsv). The API cannot search ghosts, so it will not list them.
python idm_blank_patron_tool.py wx_abc --review-exception reports/ABC/stats/ABC.[...].exception.[...].txt

# 2. In WMS Admin, for each email on the worklist: search Name, ID, Email -> open the "Not supplied" record
#    -> Delete account. (Optional API batch path with --ppid-file / --delete: see PATRON_TOOLS.md)

# 3. Rebuild patron_updates.txt for just those patrons (values come from the exception rows) and reload
python build_patron_updates.py wx_abc --exception-file reports/ABC/stats/ABC.[...].exception.[...].txt
python circ_patron_reload.py wx_abc --offline
python circ_patron_reload.py wx_abc --upload-file patrons/reloads/ABCpatronreload.txt
```

### Delete Expired Patrons
```bash
# Generate delete file for patrons expired before today (no upload)
python delete_expired_patrons.py wx_abc

# Use custom expiration date (delete on/after YYYY-MM-DD) (no upload)
python delete_expired_patrons.py wx_abc --expiration-date 2025-01-15

# Upload delete file (requires confirmation)
python delete_expired_patrons.py wx_abc --upload
```

### Review and Delete Blank Patron Records Using check_list.txt Barcode or Email Entries
```bash
# Search OCLC IDM API patrons with no name data
# IDM API key required in .env as ABC_IDM_CLIENT_ID and ABC_IDM_CLIENT_SECRET
python idm_blank_patron_tool.py wx_abc --review check_list.txt

# Open the review CSV and set confirm_delete=YES on the blank-name rows to remove
# Delete confirmed blank patron records (requires confirmation)
python idm_blank_patron_tool.py wx_abc --delete patrons/idm_review/ABC_idm_review_YYYYmmDD_HHmmSS.csv
```

## File Organization

The scripts automatically organize downloaded and processed files:

```
wms-circ-tools/
├── patrons/
│   ├── deletes/                # Generated delete files 
│   ├── downloads/              # Patron files downloaded by reload/delete scripts, or hand-edited copies
│   ├── idm_review/             # Generated output from blank patron tool
│   ├── reloads/                # Generated reload files
│   └── reports/                # Generated reports from patron scripts (skipped-patron and source-review files)
├── openrefine/                 # OpenRefine histories / export option codes (reference specs, tracked)
├── reports/
│   └── ABC/                    # Per-library folders
│       ├── items/              # Item inventory reports
│       ├── items_openrefine/   # Item inventory reports with hashmarks replaced with '[hashmark]'
│       ├── stats/              # Circulation statistics
│       ├── patrons/            # Patron reports
│       └── patrons_openrefine/ # Patron reports with hashmarks replaced with '[hashmark]'
└── logs/                       # Operation logs (delete history)
```

## Safety Features

These tools include multiple safety checks:

✅ **Fingerprint verification** - Ensures connection to legitimate OCLC server  
✅ **Data validation** - Checks barcode lengths, formats, and duplicates  
✅ **Confirmation prompts** - Requires explicit confirmation for delete operations  
✅ **Detailed logging** - Records all operations with timestamps  
✅ **Offline mode** - Test processing with existing files before uploading  
✅ **Whitespace trimming** - Prevents phantom mismatches from extra spaces

## Documentation

- **[PATRON_TOOLS.md](PATRON_TOOLS.md)** - Detailed patron script documentation with all CLI flags and examples
- **Item tools** - Coming soon

## Troubleshooting

### Connection Issues

**"Missing credentials" error**
- Verify your `.env` file has `{LIB_CODE}_USER` and `{LIB_CODE}_PASS` variables
- Check that variable names are uppercase: `WX_ABC_USER`, not `wx_abc_user`

**"Host key verification failed"**
- Run `python data_fetcher.py --print-fingerprint`
- Verify the fingerprint matches what FileZilla shows
- Update `FINGERPRINT` in your `.env` file, including SHA256: prefix

**"SSH connection failed"**
- Verify your OCLC credentials are correct
- Check your network allows connections to `sftp.oclc.org:22`
- Ensure you have active SFTP access with OCLC

### Processing Issues

Script-specific errors ("No matching barcodes found", "Column not found", "No exception report found", etc.) are covered in the **Troubleshooting** section under each script in [PATRON_TOOLS.md](PATRON_TOOLS.md). Start there; the log file in `logs/` shows which input file was chosen and every warning raised.

## Version Control Tips

**For beginners**: This project uses Git for version control. Some helpful commands:

```bash
# See what files have changed
git status

# Stage your changes
git add filename.py

# Commit with a descriptive message
git commit -m "Fix barcode validation for leading zeros"

# Push to GitHub
git push origin main
```

## License

MIT License - adapt freely for your library or consortium.

## Support

For questions about:
- **These tools**: Open a GitHub issue [open an issue](https://github.com/yourusername/your-repo/issues)!
- **OCLC SFTP/WMS**: Contact OCLC support
- **Your library's setup**: Consult your systems team
---

**Need more details?** See [PATRON_TOOLS.md](PATRON_TOOLS.md) for comprehensive documentation of all patron management scripts, CLI flags, and advanced usage examples.
