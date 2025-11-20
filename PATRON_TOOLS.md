# Patron Tools - Detailed Documentation

Comprehensive guide to patron management scripts for OCLC WMS.

## Table of Contents
- [Helper Modules](#helper-modules)
- [data_fetcher.py - Download Reports](#data_fetcherpy---download-reports)
- [circ_patron_reload.py - Patron Reloads](#circ_patron_reloadpy---patron-reloads)
- [delete_expired_patrons.py - Delete Expired Patrons](#delete_expired_patronspy---delete-expired-patrons)
- [Required Files](#required-files)
- [patron_updates.txt Format](#patron_updatestxt-format)
- [Advanced Usage](#advanced-usage)

---

## Helper Modules

These utility modules provide shared functionality across all patron tools. You don't run these directly, but understanding what they do helps when troubleshooting.

### sftp_utils.py

**Purpose**: Handles all OCLC SFTP connections and file transfers.

**Key Functions**:
- `get_credentials(lib_code)` - Loads username/password from `.env`
- `connect_sftp(user, pwd, verify=True)` - Establishes secure connection with fingerprint verification
- `download_file(sftp, remote_dir, filename, local_dir)` - Downloads files with caching (skips if already downloaded)
- `list_remote_files(sftp, remote_dir)` - Lists files in remote directory
- `print_server_fingerprint()` - Displays server fingerprint for initial setup

**When to use it directly**: You won't - other scripts import and use these functions automatically.

### file_utils.py

**Purpose**: Reads and parses various delimited file formats (CSV, TSV, pipe-delimited).

**Key Functions**:
- `safe_read_txt(path)` - Auto-detects delimiter and reads file as DataFrame
- `load_headers(headers_file)` - Loads tab-delimited column headers
- `extract_first_part_from_pipe_delimited(value, field_name)` - Extracts first segment from pipe-delimited IdM fields
- `extract_last_part_from_pipe_delimited(value, field_name)` - Extracts last segment from pipe-delimited IdM fields
- `analyze_pipe_delimited_patterns(df, column_name)` - Analyzes and warns about multi-valued fields

**Why this matters**: Your OCLC data may contain pipe-delimited values like `old_value|new_value|another_value`. This happens when your Identity Management system has been updated over time and OCLC appended new source values. By default, the scripts extract the **first** value, but you can edit them to extract the **last** (most recent) value instead. See the Advanced Usage section for instructions.

### data_loader.py

**Purpose**: Uploads processed files back to OCLC SFTP.

**Key Functions**:
- `upload_patron_reload(lib_code, local_file)` - Uploads to `/xfer/wms/in/patron` (OCLC validation required for initial test load to /xfer/wms/test/in/patron)
- `upload_patron_delete(lib_code, local_file)` - Uploads to `/xfer/wms/in/pdelete` with confirmation (OCLC validation required for initial test load to /xfer/wms/test/in/pdelete)
- `upload_file(sftp, local_path, remote_dir)` - Generic upload with validation

**Safety features**: 
- Verifies file is not empty before uploading
- Requires explicit confirmation for delete files
- Shows file statistics before upload
- Logs all upload operations

---

## data_fetcher.py - Download Reports

Downloads circulation reports and patron/item inventory files from OCLC SFTP.

### Basic Usage

```bash
python data_fetcher.py <lib_code> [options]
```

**Required**:
- `lib_code` - Your library code (e.g., `wx_abc`), which matches your `.env` credentials

### CLI Flags

| Flag | Type | Description | Default |
|------|------|-------------|---------|
| `--patrons` | flag | Download patron files | Downloads items (default) |
| `--stats` | flag | Download circulation statistics | Downloads items (default) |
| `--recent [N]` | optional int | Download N most recent files | All files if omitted; 1 if flag used without number |
| `--since DATE` | date | Only download files on/after DATE (YYYY-MM-DD or YYYYMMDD) | No date filter |
| `--print_fingerprint` | flag | Print server fingerprint and exit | Normal operation |


**Note**: `--patrons` and `--stats` are mutually exclusive. If neither is specified, the script downloads item inventory files (default).

### Examples

**Download the single most recent item file**:
```bash
python data_fetcher.py wx_abc --recent
```

**Download last 10 patron files**:
```bash
python data_fetcher.py wx_abc --patrons --recent 10
```

**Download all patron files since August 1, 2025**:
```bash
python data_fetcher.py wx_abc --patrons --since 2025-08-01
```

**Download all circulation stats since September**:
```bash
python data_fetcher.py wx_abc --stats --since 2025-09-01
```

**Get server fingerprint for initial setup**:
```bash
python data_fetcher.py --print_fingerprint
```
(No lib_code needed for this operation)

### File Patterns Recognized

**Patron files**:
- `ABC.Circulation_Patron_Report_Full.YYYYMMDD.txt`
- `ABC.Circulation_Patron_Report_Full.YYYYMMDD.csv`

**Stats files**:
- `ABC.report.YYYY-MM-DD.txt`
- `ABC.exception.YYYY-MM-DD.txt`
- `ABCD.Report_wk.YYYY-MM-DD.txt`

**Item files**:
- `ABC.Circulation_Item_Inventories.YYYYMMDD.txt`

### Output Location

Downloaded files are organized by library symbol and type:
```
reports/
└── ABC/
    ├── patrons/
    ├── stats/
    └── items/
```

### Troubleshooting

**"No matching files found"**:
- Verify your library symbol matches your OCLC data (part after the underscore in your lib_code)
- Check date filters aren't too restrictive
- Ensure files exist in `/xfer/wms/reports` on OCLC server

**"Already exists" messages**:
- The script skips files already downloaded (this is normal and saves time)
- Delete local files if you need to re-download

---

## circ_patron_reload.py - Patron Reloads

Builds OCLC-formatted patron reload files from downloaded patron data, with optional field updates.

### Basic Usage

```bash
python circ_patron_reload.py <lib_code> [options]
```

**Required**:
- `lib_code` - Your library code (e.g., `wx_abc`)

### CLI Flags

| Flag | Type | Description | Default |
|------|------|-------------|---------|
| `--offline` | flag | Use existing file in `patrons/downloads/` (skip download) | Downloads latest file |
| `--upload` | flag | Upload result to `/xfer/wms/in/patron` | Saves locally only |
| `--can-self-edit` | choice | Set default `canSelfEdit` value (`true`/`false`) | `false` |
| `--sync-illid-to-barcode` | flag | Copy updated barcodes to `illId` field (for Tipasa) | Don't sync |
| `--use-expiration-date` | flag | Apply `EXPIRATION_DATE` from `.env` to all patrons | Leave blank |
| `--use-source-value` | flag | Extract first part of pipe-delimited IdM fields | Leave blank |
| `--soft-max-barcode-len` | int | Warn if new barcodes exceed N characters | 20 |
| `--hard-max-barcode-bytes` | int | Warn if new barcodes exceed N bytes | 30 |
| `--remote-dir` | path | Remote SFTP directory | `/xfer/wms/reports` |
| `--output-dir` | path | Local output directory | `patrons` |
| `--headers-file` | path | Path to headers file | `headers_formattedpatron.txt` |
| `--project-root` | path | Directory containing `patron_updates.txt` | `.` (current directory) |
| `--pattern` | regex | Regex pattern for matching files | See below |

**Default pattern**: `^([A-Z]{3})\.Circulation_Patron_Report_Full\.(\d{8})\.txt$`

### Examples

**Basic reload (no updates, no upload)**:
```bash
python circ_patron_reload.py wx_abc
```
Creates `ABCpatronreload.txt` from latest downloaded patron file.

**Reload with barcode/email updates (no upload)**:
```bash
python circ_patron_reload.py wx_abc
```
Looks for `patron_updates.txt` in current directory and applies any updates found.

**Use latest existing file in patrons/downloads for lib_code (combine with other flags as needed to stop new download)**:
```bash
python circ_patron_reload.py wx_abc --offline
```
Processes existing file in `patrons/downloads/` - useful for testing changes.

**Upload reload file to OCLC (use latest existing file in patrons/downloads for lib_code)**:
```bash
python circ_patron_reload.py wx_abc --upload --offline
```

**Update barcodes and sync to illId (Tipasa libraries)**:
```bash
python circ_patron_reload.py wx_abc --sync-illid-to-barcode
```
Copies updated barcodes to `illId` field (required for Tipasa ILL functionality).

**Set expiration date for all patrons**:
```bash
python circ_patron_reload.py wx_abc --use-expiration-date
```
Applies date from `EXPIRATION_DATE` in `.env` file.

**Extract IdM source values**:
```bash
python circ_patron_reload.py wx_abc --use-source-value
```
⚠️ **WARNING**: Extracts first part of pipe-delimited `idAtSource` and `sourceSystem` fields. Review output carefully before uploading - old values may appear first in pipe-delimited data.

**Allow patrons to self-edit their records (enable only for true; defaults to false)**:
```bash
python circ_patron_reload.py wx_abc --can-self-edit true
```

### What Gets Updated?

**When patron_updates.txt is present**:

The script matches patron records using `patron_barcode_old` and updates any non-blank fields in your updates file. If columns for required fields not present in patron_updates.txt, values will be copied from existing data.

**Required fields -- tab-delimited columns must exist in patron_updates.txt**
- `patron_barcode_old` - Original barcode used for matching (can be same as new but column must exist in patron_updates.txt)
- `patron_barcode_new` - New barcode value (can be same as old but column must exist in patron_updates.txt)
**Required fields always included in reload files -- optional fields in patron_updates.txt**
- `familyName` - Last name
- `borrowerCategory` - Patron type
- `homeBranch` - Home library branch
- `emailAddress` - Email address

**Optional fields in patron_updates.txt** (only updated in reload files if non-blank in updates):
- `givenName` - First name
- `username` - Username
- `canSelfEdit` - Allow self-editing (`true`/`false`)
- Other patron fields as needed (mapping required in script)

**When NO patron_updates.txt**:
- Processes all patrons from downloaded file
- Applies no field updates
- Useful for initial loads or testing

### Field Mappings

The script maps OCLC circ data (patrons/downloads) fields to reload format:

| Reload Field | Source Field | Notes |
|--------------|--------------|-------|
| `barcode` | `Patron_Barcode` | Updated from patron_updates.txt |
| `givenName` | `Patron_Given_Name` | First name |
| `familyName` | `Patron_Family_Name` | Last name |
| `borrowerCategory` | `Patron_Borrower_Category` | Patron type |
| `homeBranch` | `Patron_Home_Branch_ID` | Home library |
| `emailAddress` | `Patron_Email_Address` | Email |
| `username` | `Patron_Username` | Login username |
| `institutionId` | Computed | OCLC registry ID from .env|
| `canSelfEdit` | Computed | From --can-self-edit flag |
| `oclcExpirationDate` | Computed | From .env (if --use-expiration-date) |
| `illId` | Computed | From barcode (if --sync-illid-to-barcode) |
| `idAtSource` | `Patron_User_ID_At_Source` | Only if --use-source-value |
| `sourceSystem` | `Patron_Source_System` | Only if --use-source-value |

**Excluded fields** (always blank):
- Address fields (primary/secondary)
- Phone numbers (except email)
- Birth date, gender
- Photo URL, custom data fields
- Most ILL fields

### Validation Checks

The script validates new barcodes against OCLC requirements:

✅ **Non-empty** - Warns and skips empty barcode values  
✅ **Length limits** - Warns if >20 characters (soft) or >30 bytes (hard)  
✅ **No duplicates** - Warns if new barcodes appear multiple times  
✅ **No collisions** - Warns if new barcode already exists elsewhere in file  
✅ **Reserved characters** - Warns if URL-encoding characters present (`!*'();:@&=+$,/?%#[]`)

### Output Location

Processed files are saved to:
```
patrons/
├── downloads/           # Downloaded source files
└── reloads/
    └── ABCpatronreload.txt  # Generated reload file
```

### Troubleshooting

**"circ_patron_reload.py: error: the following arguments are required: lib_code"**
- You need to include lib_code (wx_OCLC-symbol, ex. wx_acacl) in the command

**"No matching barcodes found in patron_updates.txt"**:
- Possibly using the wrong library code or wrong update file
- Check that barcodes in updates file match the barcodes in patrons/downloads/ABC.* data
- Verify you downloaded the correct library's patron file

**"Column not found: Patron_Barcode"**:
- Your downloaded file doesn't have the expected structure
- Ensure you downloaded a "Full" patron report, not a partial report
- Verify that columns in updates file have headers that match headers_formattedpatron.txt
- Verify that columns in updates file are mapped (not excluded) in circ_patron_reload.py

**Warning about pipe-delimited values**:
- Your source system data has multiple values (e.g., `old_id|new_id`)
- Review the output file before uploading to ensure correct values are used
- Use `--use-source-value` only after reviewing the log warnings

---

## delete_expired_patrons.py - Delete Expired Patrons

Generates patron delete files for accounts expiring before a cutoff date.

### Basic Usage

```bash
python delete_expired_patrons.py <lib_code> [options]
```

**Required**:
- `lib_code` - Your library code (e.g., `wx_abc`)

### CLI Flags

| Flag | Type | Description | Default |
|------|------|-------------|---------|
| `--offline` | flag | Use existing file in `patrons/downloads/` | Downloads latest file |
| `--upload` | flag | Upload delete file to `/xfer/wms/in/pdelete` (requires confirmation) | Saves locally only |
| `--expiration-date` | date | Custom expiration cutoff (YYYY-MM-DD) | Today |
| `--sync-illid-to-barcode` | flag | Copy barcode to `illId` field (Tipasa libraries only) | Don't sync |
| `--use-source-value` | flag | Include IdM source fields in delete file | Leave blank |
| `--remote-dir` | path | Remote SFTP directory | `/xfer/wms/reports` |
| `--output-dir` | path | Local output directory | `patrons` |
| `--headers-file` | path | Path to delete headers file | `headers_deletes.txt` |
| `--barcode-column` | name | Column name for patron barcode | `Patron_Barcode` |
| `--expiration-column` | name | Column name for expiration date | `Patron_Expiration_Date` |
| `--print-fingerprint` | flag | Print server fingerprint and exit | Normal operation |

### Examples

**Generate delete file for patrons expired today**:
```bash
python delete_expired_patrons.py wx_abc
```
Creates `ABCpatronsdelete_MMDDYY.txt` with patrons expiring before today.

**Use custom expiration date**:
```bash
python delete_expired_patrons.py wx_abc --expiration-date 2025-01-15
```
Deletes patrons expiring before January 15, 2025.

**Test with existing file (no download)**:
```bash
python delete_expired_patrons.py wx_abc --offline
```
Uses most recent file in `patrons/downloads/`.

**Upload delete file to OCLC** (⚠️ This permanently deletes patrons):
```bash
python delete_expired_patrons.py wx_abc --upload
```
You'll be prompted to type 'yes' to confirm. Shows file statistics before upload.

**For Tipasa libraries (include illId)**:
```bash
python delete_expired_patrons.py wx_abc --sync-illid-to-barcode
```

**Include IdM source identifiers**:
```bash
python delete_expired_patrons.py wx_abc --use-source-value
```
By default, this extracts the **first** value from pipe-delimited source fields. If you need the **last** (most recent) value instead, you can edit the script using the same process as described in the "Extracting IdM Source Values" section under Advanced Usage - just edit `delete_expired_patrons.py` instead of `circ_patron_reload.py`.

### Delete File Format

Generated delete files contain 5 fields:

| Field | Source | Notes |
|-------|--------|-------|
| `institutionId` | From `.env` (e.g., `ABC_INSTITUTION_ID`) | Your library's WMS institution ID |
| `barcode` | `Patron_Barcode` | Patron barcode to delete |
| `sourceSystem` | `Patron_Source_System` | Blank unless `--use-source-value` |
| `idAtSource` | `Patron_User_ID_At_Source` | Blank unless `--use-source-value` |
| `illId` | From barcode | Blank unless `--sync-illid-to-barcode` |

**Example delete file**:
```
institutionId	barcode	sourceSystem	idAtSource	illId
12345	987654321			
12345	111222333			
```

### Institution ID Setup

Your `.env` file must contain the institution ID for delete operations:

```env
ABC_INSTITUTION_ID=12345
```

Find your institution ID in OCLC WMS under Settings → Library Information.

### Output Location

Delete files are saved to:
```
patrons/
├── downloads/              # Downloaded source files
└── deletes/
    └── ABCpatronsdelete_MMDDYY.txt  # Generated delete file
```

Logs are saved to:
```
logs/
└── ABCpatronsdelete_MMDDYY.log  # Detailed operation log
```

### Safety Confirmation

When using `--upload`, you'll see a confirmation prompt:

```
============================================================
⚠️  UPLOAD CONFIRMATION REQUIRED
============================================================
Delete file: ABCpatronsdelete_100725.txt
Location:    patrons/deletes/ABCpatronsdelete_100725.txt

File size:         1,234 bytes
Records to delete: 156 patrons

Destination: /xfer/wms/in/pdelete/

⚠️  WARNING: This will PERMANENTLY DELETE patron records!
Please review the file carefully before proceeding.

Type 'yes' to upload, 'no' to cancel:
```

**IMPORTANT**: Review your delete file before uploading! Once processed by OCLC, deletions cannot be undone.

### Troubleshooting

**"Institution ID not found"**:
- Add `ABC_INSTITUTION_ID=12345` to your `.env` file
- Replace `ABC` with your library symbol (uppercase)
- Replace `12345` with your actual WMS institution ID

**"No patron files found"**:
- Ensure you've downloaded a patron file first using `data_fetcher.py`
- Check that the file matches your library symbol
- Try without `--offline` to download a fresh file

**"Column not found: Patron_Expiration_Date"**:
- Your patron file doesn't include expiration dates
- Ensure you're using a "Full" patron report from OCLC

---

## Required Files

### headers_formattedpatron.txt

**Purpose**: Defines the 46-column structure for OCLC patron reload files.

**Format**: Tab-delimited, single line with column names.

**Columns** (in order):
```
prefix	givenName	middleName	familyName	suffix	nickname	canSelfEdit	dateOfBirth	gender	institutionId	barcode	idAtSource	sourceSystem	borrowerCategory	circRegistrationDate	oclcExpirationDate	homeBranch	primaryStreetAddressLine1	primaryStreetAddressLine2	primaryCityOrLocality	primaryStateOrProvince	primaryPostalCode	primaryCountry	primaryPhone	secondaryStreetAddressLine1	secondaryStreetAddressLine2	secondaryCityOrLocality	secondaryStateOrProvince	secondaryPostalCode	secondaryCountry	secondaryPhone	emailAddress	mobilePhone	notificationEmail	notificationTextPhone	patronNotes	photoURL	customdata1	customdata2	customdata3	customdata4	username	illId	illApprovalStatus	illPatronType	illPickupLocation
```

**Do not modify** this file unless OCLC changes their patron reload format.

### headers_deletes.txt

**Purpose**: Defines the 5-column structure for OCLC patron delete files.

**Format**: Tab-delimited, single line with column names.

**Columns** (in order):
```
institutionId	barcode	sourceSystem	idAtSource	illId
```

**Do not modify** this file unless OCLC changes their delete file format.

---

## patron_updates.txt Format

Optional file for updating patron fields during reload operations. If this file doesn't exist, `circ_patron_reload.py` processes all patrons without updates.

### File Structure

**Format**: Tab-delimited text file with headers

**Required column**:
- `patron_barcode_old` - Identifies which patrons to update

**Optional update columns** (include only the fields you want to update):
- `patron_barcode_new` - New barcode (can be same as old)
- `familyName` - Last name
- `givenName` - First name
- `borrowerCategory` - Patron type
- `homeBranch` - Home library branch ID
- `emailAddress` - Email address
- `username` - Username
- `canSelfEdit` - Allow self-editing (`true` or `false`)

### Example: Barcode Changes

```
patron_barcode_old	patron_barcode_new
123456	AB123456
789012	CD789012
```

### Example: Multiple Field Updates

```
patron_barcode_old	patron_barcode_new	emailAddress	borrowerCategory
123456	AB123456	newemail@example.com	FACULTY
789012	CD789012	another@example.com	STUDENT
```

### Example: Keep Same Barcode, Update Other Fields

```
patron_barcode_old	emailAddress	homeBranch
123456	updated@example.com	MAIN
789012	changed@example.com	BRANCH
```

### Update Behavior

**Required fields**: Always updated (even if blank in your file):
- `familyName`, `borrowerCategory`, `homeBranch`, `emailAddress`

**Optional fields**: Only updated if non-blank in your file:
- `givenName`, `username`, etc.

This means you can update barcodes without worrying about overwriting first names if you leave `givenName` blank - the original OCLC value will be preserved.

### Creating patron_updates.txt

**Method 1: Excel/Google Sheets**
1. Create spreadsheet with columns
2. Export as "Tab Delimited Text (.txt)"
3. Save as `patron_updates.txt` in your project directory

**Method 2: Text Editor**
1. Create file with tab-separated values
2. Use actual TAB characters (not spaces)
3. Save as `patron_updates.txt`

**Method 3: Export from ILS**
1. Export patron data from your ILS
2. Format with required columns
3. Save as tab-delimited text

### Validation

The script automatically validates barcode updates:

✅ Checks for empty barcodes (warns and skips)  
✅ Warns if barcodes exceed 20 characters  
✅ Warns if barcodes exceed 30 bytes  
✅ Detects duplicate new barcodes  
✅ Warns if new barcode already exists in file  
✅ Flags reserved URL characters

Review all warnings in the log before uploading your reload file.

### Tipasa Libraries

If you use Tipasa for interlibrary loan, add `--sync-illid-to-barcode` flag:

```bash
python circ_patron_reload.py wx_abc --sync-illid-to-barcode
```

This copies updated barcodes to the `illId` field (required for Tipasa functionality).

### Backward Compatibility

The script also recognizes the old filename `barcode_updates.txt` for backward compatibility, but `patron_updates.txt` is preferred.

---

## Advanced Usage

### Offline Mode for Testing

Both `circ_patron_reload.py` and `delete_expired_patrons.py` support `--offline` mode:

```bash
# Process existing file without downloading
python circ_patron_reload.py wx_abc --offline
python delete_expired_patrons.py wx_abc --offline
```

This is useful for:
- Testing your `patron_updates.txt` without re-downloading
- Processing files multiple times with different flags
- Working without internet connection
- Avoiding API rate limits during development

### Custom Date Ranges

Download files within a specific date range:

```bash
# Get September patron files only
python data_fetcher.py wx_abc --patrons --since 2025-09-01 --recent 30
```

### Batch Processing Multiple Libraries

Create a shell script to process multiple libraries:

```bash
#!/bin/bash
# process_all_libraries.sh

for lib in wx_abc wx_def wx_xyz; do
    echo "Processing $lib..."
    python circ_patron_reload.py $lib --upload
done
```

Make executable and run:
```bash
chmod +x process_all_libraries.sh
./process_all_libraries.sh
```

### Extracting IdM Source Values

If your library uses an Identity Management system and OCLC has multiple pipe-delimited values:

```bash
python circ_patron_reload.py wx_abc --use-source-value
```

⚠️ **WARNING**: By default, this extracts the **FIRST** part of pipe-delimited values (e.g., `old_id|new_id` → `old_id`). If your IdM system has been updated and OCLC appended new values, old values may appear first. Always review the log output showing what will be extracted:

```
=============================================================
idAtSource: Found 234 records (45.2%) with multiple values
Script will extract FIRST part only
REVIEW OUTPUT before uploading to ensure correct values!
=============================================================
```

The script shows sample values before and after extraction. Review your output file in `patrons/reloads/` before uploading.

#### Extracting the Last (Newest) Value Instead

If you need the **last** value from pipe-delimited data (e.g., `old_id|new_id` → `new_id`), you'll need to make a small edit to the script. This is useful when OCLC appends newer values to the end.

**Step-by-step instructions:**

1. **Open the script in a text editor:**
   - File to edit: `circ_patron_reload.py`
   - Use any text editor (Notepad++, VS Code, or even Windows Notepad)

2. **Find the function to change:**
   - Use your editor's Find feature (Ctrl+F or Cmd+F)
   - Search for: `def process_special_fields`
   - You'll find a function that starts around line 380

3. **Locate the extraction calls:**
   Inside `process_special_fields`, you'll see two lines like this:
   ```python
   df["Patron_User_ID_At_Source"] = df["Patron_User_ID_At_Source"].apply(
       lambda x: extract_first_part_from_pipe_delimited(x, "idAtSource")
   )
   ```

4. **Change "first" to "last":**
   Replace `extract_first_part_from_pipe_delimited` with `extract_last_part_from_pipe_delimited`:
   
   **Before:**
   ```python
   lambda x: extract_first_part_from_pipe_delimited(x, "idAtSource")
   ```
   
   **After:**
   ```python
   lambda x: extract_last_part_from_pipe_delimited(x, "idAtSource")
   ```

5. **Make the same change for sourceSystem:**
   A few lines down, find and change the sourceSystem line:
   
   **Before:**
   ```python
   most_common_value = find_most_common_source_system(df, "Patron_Source_System")
   ```
   
   This line finds the most common FIRST value. If you want the most common LAST value, you'd need to modify `find_most_common_source_system` in the same way, but typically sourceSystem stays constant, so this change is less common.

6. **Save the file** and run your script normally:
   ```bash
   python circ_patron_reload.py wx_abc --use-source-value
   ```

**Important notes:**
- The function `extract_last_part_from_pipe_delimited` already exists in `file_utils.py` - you're just telling the script to use it instead of `extract_first_part_from_pipe_delimited`
- Always test with `--offline` first to see the results before uploading
- Review the log output to verify you're getting the correct values
- Consider documenting your change with a comment in the code

**Example of what changes:**
- Input: `urn:mace:oclc:idm:old_system|urn:mace:oclc:idm:new_system`
- Extract FIRST: `urn:mace:oclc:idm:old_system`
- Extract LAST: `urn:mace:oclc:idm:new_system` ✅

**For beginners:** If you're not comfortable editing Python code, ask a colleague or your IT department to help with this change. It's a simple find-and-replace, but it's important to get it right.

### Setting Expiration Dates

Add to your `.env` file:

```env
# Set specific expiration date
EXPIRATION_DATE=2026-12-31

# Or disable expiration dates
EXPIRATION_DATE=IGNORE
```

Then use the flag:

```bash
python circ_patron_reload.py wx_abc --use-expiration-date
```

### Logging

All operations are logged. Check logs if something goes wrong:

```
logs/
└── ABCpatronsdelete_MMDDYY.log
```

Logs include:
- Timestamp for each operation
- Files downloaded/processed
- Validation warnings
- Errors with details
- Upload confirmations

### Version Control Best Practices

**Always commit before major changes**:
```bash
git add .
git commit -m "Before processing ABC library patron updates"
```

**Don't commit sensitive data**:
Add to your `.gitignore`:
```
.env
patron_updates.txt
patrons/
reports/
logs/
```

**Document your workflow**:
Add notes to your commit messages about which library and what operation.

---

## Need Help?

- **Script errors**: Check the log files in `logs/`
- **OCLC questions**: Contact OCLC support
- **GitHub issues**: Open an issue with your error message
- **Security concerns**: Verify fingerprint before trusting connections

