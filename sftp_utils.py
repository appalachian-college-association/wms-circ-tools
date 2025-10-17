"""
Shared SFTP utilities for OCLC WMS Circulation Tools
-----------------------------------------------------
This module provides common functions for:
- Loading credentials from environment variables
- Connecting to OCLC SFTP with fingerprint verification
- Downloading files
- Listing remote directories

Used by: data_fetcher.py, circ_patron_reload.py, delete_expired_patrons.py
"""

import os
import base64
import hashlib
import logging
from typing import Tuple
from pathlib import Path

import paramiko
from dotenv import load_dotenv

# Load environment variables once when module is imported
load_dotenv()

# SFTP connection constants from .env
EXPECTED_FINGERPRINT = os.getenv("FINGERPRINT", "")
HOST = os.getenv("HOST_NAME", "")
PORT = int(os.getenv("HOST_PORT", "22"))

# Set up module logger
logger = logging.getLogger(__name__)


def get_credentials(lib_code: str) -> Tuple[str, str]:
    """
    Get username and password from environment variables.
    
    Args:
        lib_code: Library code like 'wx_tdt' or 'WX_TDT'
        
    Returns:
        Tuple of (username, password)
        
    Raises:
        ValueError: If credentials not found in environment
        
    Example:
        user, pwd = get_credentials('wx_tdt')
        # Looks for WX_TDT_USER and WX_TDT_PASS in .env
    """
    key = lib_code.upper()
    user = os.getenv(f"{key}_USER")
    pwd = os.getenv(f"{key}_PASS")
    
    if not user or not pwd:
        raise ValueError(
            f"Missing credentials for {lib_code}. "
            f"Set {key}_USER and {key}_PASS in .env file"
        )
    
    return user, pwd


def get_server_fingerprint(ssh: paramiko.SSHClient) -> str:
    """
    Calculate SHA256 fingerprint of remote server's host key.
    
    Args:
        ssh: Connected paramiko SSHClient
        
    Returns:
        Fingerprint string in format 'SHA256:...'
    """
    host_key = ssh.get_transport().get_remote_server_key()
    sha256_digest = hashlib.sha256(host_key.asbytes()).digest()
    return "SHA256:" + base64.b64encode(sha256_digest).decode()


def verify_fingerprint(ssh: paramiko.SSHClient) -> bool:
    """
    Verify server fingerprint matches expected value from .env.
    
    Args:
        ssh: Connected paramiko SSHClient
        
    Returns:
        True if fingerprint matches or no expected fingerprint set
        False if mismatch detected
        
    Note:
        If EXPECTED_FINGERPRINT is not set in .env, returns True (no verification)
    """
    if not EXPECTED_FINGERPRINT:
        logger.warning("No FINGERPRINT set in .env - skipping verification")
        return True
    
    actual = get_server_fingerprint(ssh)
    
    if actual != EXPECTED_FINGERPRINT:
        logger.error(
            "Fingerprint mismatch!\n"
            f"Expected: {EXPECTED_FINGERPRINT}\n"
            f"Got:      {actual}"
        )
        return False
    
    logger.info("Host key verified successfully")
    return True


def connect_sftp(username: str, password: str, 
                 verify: bool = True) -> Tuple[paramiko.SSHClient, paramiko.SFTPClient]:
    """
    Connect to OCLC SFTP server with optional fingerprint verification.
    
    Args:
        username: SFTP username
        password: SFTP password
        verify: Whether to verify server fingerprint (default True)
        
    Returns:
        Tuple of (ssh_client, sftp_client) - both must be closed by caller
        
    Raises:
        RuntimeError: If connection fails or fingerprint mismatch
        
    Example:
        ssh, sftp = connect_sftp(user, pwd)
        try:
            # do work with sftp
            files = sftp.listdir()
        finally:
            sftp.close()
            ssh.close()
    """
    if not HOST:
        raise RuntimeError("HOST_NAME not set in .env file")
    
    logger.info(f"Connecting to {HOST}:{PORT} as {username}")
    
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        ssh.connect(
            hostname=HOST,
            port=PORT,
            username=username,
            password=password,
            look_for_keys=False,
            allow_agent=False
        )
        
        # Verify fingerprint if requested
        if verify and not verify_fingerprint(ssh):
            ssh.close()
            raise RuntimeError("Host fingerprint verification failed")
        
        logger.info("SSH connection established")
        sftp = ssh.open_sftp()
        
        return ssh, sftp
        
    except Exception as e:
        if ssh:
            ssh.close()
        raise RuntimeError(f"SFTP connection failed: {e}")


def list_remote_files(sftp: paramiko.SFTPClient, remote_dir: str) -> list:
    """
    List files in a remote SFTP directory.
    
    Args:
        sftp: Connected SFTP client
        remote_dir: Path to remote directory
        
    Returns:
        List of filenames (strings)
        
    Example:
        files = list_remote_files(sftp, '/xfer/wms/reports')
    """
    sftp.chdir(remote_dir)
    return sftp.listdir()


def download_file(sftp: paramiko.SFTPClient, 
                  remote_dir: str,
                  filename: str, 
                  local_dir: Path) -> Path:
    """
    Download a single file from SFTP to local directory.
    
    Args:
        sftp: Connected SFTP client
        remote_dir: Remote directory path
        filename: Name of file to download
        local_dir: Local directory to save to (will be created if needed)
        
    Returns:
        Path to downloaded local file
        
    Raises:
        FileNotFoundError: If remote file doesn't exist
        ValueError: If downloaded file is empty
        
    Example:
        local_path = download_file(sftp, '/xfer/wms/reports', 
                                   'TDT.report.txt', Path('reports/TDT'))
    
    Note:
        - Skips download if local file already exists with content
        - Removes and re-downloads if local file exists but is empty
        - Verifies file size after download
    """
    # Create local directory if needed
    local_dir.mkdir(parents=True, exist_ok=True)
    local_path = local_dir / filename
    
    # Build remote path - handle both Unix-style paths from SFTP
    remote_path = remote_dir.rstrip('/\\') + '/' + filename
    
    logger.info(f"Downloading: {remote_path}")
    logger.info(f"         to: {local_path}")
    
    # Check if we already have this file locally
    if local_path.exists():
        local_size = local_path.stat().st_size
        
        if local_size > 0:
            logger.info(f"Using cached file (skipping download): {filename}")
            return local_path
        else:
            logger.warning(f"Local file empty, re-downloading: {filename}")
            local_path.unlink()  # Delete empty file
    
    try:
        # Verify remote file exists and get size
        remote_stat = sftp.stat(remote_path)
        remote_size = remote_stat.st_size
        
        if remote_size == 0:
            raise ValueError(f"Remote file is empty: {remote_path}")
        
        logger.info(f"Remote file size: {remote_size:,} bytes")
        
        # Download the file
        sftp.get(remote_path, str(local_path))
        
        # Verify download
        downloaded_size = local_path.stat().st_size
        
        if downloaded_size == 0:
            local_path.unlink()
            raise ValueError(f"Downloaded file is empty: {filename}")
        
        if downloaded_size != remote_size:
            logger.warning(
                f"Size mismatch - Remote: {remote_size:,}, "
                f"Local: {downloaded_size:,}"
            )
        
        logger.info(f"Download complete: {filename} ({downloaded_size:,} bytes)")
        return local_path
        
    except FileNotFoundError:
        logger.error(f"Remote file not found: {remote_path}")
        
        # List available files for debugging
        try:
            files = sftp.listdir(remote_dir)
            logger.info(f"Files in {remote_dir}:")
            for f in sorted(files)[:10]:  # Show first 10
                logger.info(f"  {f}")
        except Exception as e:
            logger.error(f"Could not list directory: {e}")
        
        raise FileNotFoundError(f"Remote file not found: {remote_path}")
    
    except Exception as e:
        logger.error(f"Download failed: {e}")
        if local_path.exists():
            local_path.unlink()  # Clean up partial download
        raise


def print_server_fingerprint() -> None:
    """
    Connect to server and print fingerprint for .env configuration.
    
    This is a utility function to discover the server's fingerprint
    on first connection. Use --print-fingerprint flag in scripts.
    
    Prints:
        Server fingerprint to console with setup instructions
    """
    print("\n" + "="*60)
    print("DISCOVERING SERVER FINGERPRINT")
    print("="*60)
    
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        # Connect with dummy credentials just to get fingerprint
        ssh.connect(
            hostname=HOST,
            port=PORT,
            username="dummy",
            password="dummy",
            look_for_keys=False,
            allow_agent=False
        )
        
    except paramiko.AuthenticationException:
        # Authentication fails but we got the fingerprint!
        if ssh.get_transport():
            fingerprint = get_server_fingerprint(ssh)
            print(f"\nServer fingerprint: {fingerprint}")
            print("\nNext steps:")
            print("1. Add this line to your .env file:")
            print(f"   FINGERPRINT={fingerprint}")
            print("2. Run your script again with proper credentials")
            print("="*60)
            ssh.close()
            return
    
    except Exception as e:
        print(f"Connection error: {e}")
        print("="*60)
        return
    
    finally:
        if ssh:
            ssh.close()
    
    print("Could not retrieve fingerprint")
    print("="*60)
