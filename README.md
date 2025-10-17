# OCLC WMS Circulation Tools

Python tools for downloading, processing, and managing OCLC WorldShare Management Services (WMS) circulation data.

## Overview

This repository provides utilities to:
- **Download** circulation reports and patron/item data from OCLC SFTP
- **Process** patron records for reload operations (updates, barcode changes)
- **Generate** patron delete files for expired accounts
- **Manage** patron data with validation and safety checks

These tools are designed for library staff working with WMS circulation data, with varying levels of Python experience. Detailed documentation and examples are provided for each tool.

## What's Included

### Patron Management Tools
- **`data_fetcher.py`** - Download circulation reports and patron files from OCLC SFTP
- **`circ_patron_reload.py`** - Build patron reload files with optional updates (barcodes, email, etc.)
- **`delete_expired_patrons.py`** - Generate delete files for expired patron accounts

### Helper Modules
- **`sftp_utils.py`** - Shared SFTP connection and file transfer functions
- **`file_utils.py`** - File reading, parsing, and data cleaning utilities
- **`data_loader.py`** - Upload processed files back to OCLC sFTP

### Configuration Files
- **`headers_formattedpatron.txt`** - Column headers for patron reload files (46 fields)
- **`headers_deletes.txt`** - Column headers for patron delete files (5 fields)
- **`patron_updates.txt`** - User-supplied file for patron field updates (optional - use headers_formattedpatron.txt column names (except barcodes old/new) and verify/update mapping in circ_patron_reload.py)

📖 **Detailed documentation**: See [PATRON_TOOLS.md](PATRON_TOOLS.md) for comprehensive usage instructions.

## Quick Start

### Requirements
- **Python 3.9+**
- **Modules**: `paramiko`, `python-dotenv`, `pandas`

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
```

### Update Patron Records
```bash
# Create reload file with updates from patron_updates.txt (no upload)
python circ_patron_reload.py wx_abc

# Use most recent file from patrons/downloads/ABC.*.txt (no upload)
python circ_patron_reload.py wx_abc --offline

# Upload reload file from patrons/reloads/ABC.*.txt to OCLC
python circ_patron_reload.py wx_abc --upload
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

## File Organization

The scripts automatically organize downloaded and processed files:

```
wms-circ-tools/
├── patrons/
│   ├── downloads/        # Downloaded patron files
│   ├── reloads/          # Generated reload files
│   └── deletes/          # Generated delete files
├── reports/
│   └── ABC/              # Per-library folders
│       ├── items/        # Item inventory reports
│       ├── stats/        # Circulation statistics
│       └── patrons/      # Patron reports
└── logs/                 # Operation logs (delete history)
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
- Run `python data_fetcher.py --print_fingerprint`
- Verify the fingerprint matches what FileZilla shows
- Update `FINGERPRINT` in your `.env` file, including SHA256: prefix

**"SSH connection failed"**
- Verify your OCLC credentials are correct
- Check your network allows connections to `sftp.oclc.org:22`
- Ensure you have active SFTP access with OCLC

### Processing Issues

**"No matching barcodes found in patron_updates.txt"**
- You may be using the wrong library's update file
- Verify the barcodes in `patron_updates.txt` match your downloaded patron file
- Check for extra whitespace (the script trims automatically but warns you)

**"Column not found" errors**
- Your downloaded file may have a different structure than expected
- Check that you're using the correct report type in patrons/downloads (Full patron report)
- Check that column names match headers_formattedpatron.txt (except patron_barcode_old - required - and patron_barcode_new) and verify/update mapping in circ_patron_reload.py. 
- See [PATRON_TOOLS.md](PATRON_TOOLS.md) for additional details.

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
