"""
Data upload utilities for OCLC WMS Circulation Tools
-----------------------------------------------------
This module provides functions for uploading processed files
back to OCLC SFTP server.

Common upload destinations:
- /xfer/wms/in/patron  - Patron reload files
- /xfer/wms/in/pdelete - Patron delete files

Used by: circ_patron_reload.py, delete_expired_patrons.py
"""

import logging
from pathlib import Path
from typing import Optional

import paramiko

# Import our shared SFTP utilities
from sftp_utils import get_credentials, connect_sftp

# Set up module logger
logger = logging.getLogger(__name__)


def upload_file(sftp: paramiko.SFTPClient,
                local_path: Path,
                remote_dir: str,
                remote_filename: Optional[str] = None) -> str:
    """
    Upload a local file to OCLC SFTP server.
    
    Args:
        sftp: Connected SFTP client
        local_path: Path to local file to upload
        remote_dir: Remote directory to upload to
        remote_filename: Optional custom filename (uses local name if not provided)
        
    Returns:
        Full remote path of uploaded file
        
    Raises:
        FileNotFoundError: If local file doesn't exist
        ValueError: If local file is empty
        
    Example:
        remote_path = upload_file(
            sftp, 
            Path('outputs/processed/TDTpatronreload.txt'),
            '/xfer/wms/in/patron'
        )
    """
    # Verify local file exists
    if not local_path.exists():
        raise FileNotFoundError(f"Local file not found: {local_path}")
    
    # Verify file has content
    file_size = local_path.stat().st_size
    if file_size == 0:
        raise ValueError(f"Cannot upload empty file: {local_path}")
    
    # Use original filename if not specified
    if remote_filename is None:
        remote_filename = local_path.name
    
    # Build remote path
    remote_path = remote_dir.rstrip('/\\') + '/' + remote_filename
    
    logger.info(f"Uploading: {local_path}")
    logger.info(f"       to: {remote_path}")
    logger.info(f"     Size: {file_size:,} bytes")
    
    try:
        sftp.put(str(local_path), remote_path)
        logger.info("Upload successful")
        return remote_path
        
    except Exception as e:
        logger.error(f"Upload failed: {e}")
        raise


def upload_patron_reload(lib_code: str, 
                        local_file: Path,
                        verify_fingerprint: bool = True,
                        remote_dir: str = '/xfer/wms/in/patron') -> str:
    """
    Upload a patron reload file to OCLC.
    
    This is a convenience function that:
    1. Gets credentials
    2. Connects to SFTP
    3. Uploads to /xfer/wms/in/patron
    4. Closes connection
    
    Args:
        lib_code: Library code (e.g., 'wx_tdt')
        local_file: Path to patron reload file
        verify_fingerprint: Whether to verify server fingerprint
        
    Returns:
        Remote path of uploaded file
        
    Example:
        upload_patron_reload('wx_tdt', Path('outputs/processed/TDTpatronreload.txt'))
    """
    user, pwd = get_credentials(lib_code)
    ssh, sftp = connect_sftp(user, pwd, verify=verify_fingerprint)
    
    try:
        remote_path = upload_file(sftp, local_file, remote_dir)
        return remote_path
    finally:
        sftp.close()
        ssh.close()
        logger.info("Connection closed")


def upload_patron_delete(lib_code: str,
                        local_file: Path,
                        verify_fingerprint: bool = True,
                        require_confirmation: bool = True) -> Optional[str]:
    """
    Upload a patron delete file to OCLC with optional confirmation.
    
    ⚠️  WARNING: This permanently deletes patron records!
    
    Args:
        lib_code: Library code (e.g., 'wx_kqy')
        local_file: Path to delete file
        verify_fingerprint: Whether to verify server fingerprint
        require_confirmation: If True, asks user to confirm before upload
        
    Returns:
        Remote path of uploaded file, or None if upload cancelled
        
    Example:
        # With confirmation prompt
        upload_patron_delete('wx_kqy', Path('outputs/processed/KQYpatronsdelete_010125.txt'))
        
        # Skip confirmation (dangerous!)
        upload_patron_delete('wx_kqy', delete_file, require_confirmation=False)
    """
    # Show file details and get confirmation if required
    if require_confirmation:
        if not confirm_delete_upload(local_file):
            logger.info("Upload cancelled by user")
            return None
    
    user, pwd = get_credentials(lib_code)
    ssh, sftp = connect_sftp(user, pwd, verify=verify_fingerprint)
    
    try:
        remote_path = upload_file(sftp, local_file, '/xfer/wms/in/pdelete')
        return remote_path
    finally:
        sftp.close()
        ssh.close()
        logger.info("Connection closed")


def confirm_delete_upload(delete_file: Path) -> bool:
    """
    Ask user to confirm patron delete file upload.
    
    Shows file details and requires explicit 'yes' to proceed.
    
    Args:
        delete_file: Path to delete file
        
    Returns:
        True if user confirms, False otherwise
    """
    print(f"\n{'='*60}")
    print("UPLOAD CONFIRMATION REQUIRED")
    print(f"{'='*60}")
    print(f"Delete file: {delete_file.name}")
    print(f"Location:    {delete_file}")
    
    # Show file statistics
    try:
        file_size = delete_file.stat().st_size
        
        # Count records (subtract 1 for header)
        with open(delete_file, 'r', encoding='utf-8') as f:
            record_count = sum(1 for line in f) - 1
        
        print(f"\nFile size:         {file_size:,} bytes")
        print(f"Records to delete: {record_count:,} patrons")
        
    except Exception as e:
        print(f"\nCould not read file details: {e}")
    
    print(f"\nDestination: /xfer/wms/in/pdelete/")
    print("\nWARNING: This will PERMANENTLY DELETE patron records!")
    print("Please review the file carefully before proceeding.")
    
    # Get user confirmation
    while True:
        response = input("\nType 'yes' to upload, 'no' to cancel: ").strip().lower()
        
        if response == 'yes':
            return True
        elif response == 'no':
            return False
        else:
            print("Please type 'yes' or 'no'")


def upload_with_connection(sftp: paramiko.SFTPClient,
                          files: list[tuple[Path, str]]) -> list[str]:
    """
    Upload multiple files using an existing SFTP connection.
    
    Useful when you need to upload several files and want to
    reuse the same connection instead of reconnecting for each file.
    
    Args:
        sftp: Connected SFTP client
        files: List of (local_path, remote_dir) tuples
        
    Returns:
        List of remote paths for successfully uploaded files
        
    Example:
        files_to_upload = [
            (Path('file1.txt'), '/xfer/wms/in/patron'),
            (Path('file2.txt'), '/xfer/wms/in/patron'),
        ]
        
        ssh, sftp = connect_sftp(user, pwd)
        try:
            remote_paths = upload_with_connection(sftp, files_to_upload)
        finally:
            sftp.close()
            ssh.close()
    """
    uploaded = []
    
    for local_path, remote_dir in files:
        try:
            remote_path = upload_file(sftp, local_path, remote_dir)
            uploaded.append(remote_path)
        except Exception as e:
            logger.error(f"Failed to upload {local_path}: {e}")
            # Continue with other files
    
    return uploaded
