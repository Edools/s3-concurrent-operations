#!/usr/bin/env python3
"""
S3 Concurrent File Uploader

This script uploads multiple files to an S3 bucket concurrently using AWS CLI.
It provides progress tracking, error handling, and configurable concurrency levels.

Requirements:
- AWS CLI installed and configured
- Python 3.6+

Usage:
    python s3_concurrent_uploader.py --bucket my-bucket --files file1.txt file2.txt file3.txt
    python s3_concurrent_uploader.py --bucket my-bucket --directory /path/to/files
    python s3_concurrent_uploader.py --bucket my-bucket --files *.txt --workers 10
"""

import argparse
import glob
import os
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path


class S3Uploader:
    def __init__(self, bucket_name, max_workers=5, s3_prefix="", aws_profile=None):
        """
        Initialize S3 Uploader

        Args:
            bucket_name (str): Name of the S3 bucket
            max_workers (int): Maximum number of concurrent uploads
            s3_prefix (str): Prefix to add to S3 object keys
            aws_profile (str): AWS profile to use (optional)
        """
        self.bucket_name = bucket_name
        self.max_workers = max_workers
        self.s3_prefix = s3_prefix.rstrip("/")
        self.aws_profile = aws_profile

        self.lock = threading.Lock()
        self.uploaded_count = 0
        self.failed_count = 0
        self.total_files = 0

        # Check if AWS CLI is available
        self._check_aws_cli()

    def _check_aws_cli(self):
        """Check if AWS CLI is installed and configured"""
        try:
            cmd = ["aws", "--version"]
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=10, check=False
            )
            if result.returncode != 0:
                raise Exception("AWS CLI not found or not working")
            print(f"✓ AWS CLI detected: {result.stdout.strip()}")
        except Exception as e:
            print(f"❌ Error: AWS CLI is required but not available: {e}")
            sys.exit(1)

    def _build_aws_command(self, local_file, s3_key):
        """Build AWS CLI command for file upload"""
        cmd = ["aws", "s3", "cp", local_file, f"s3://{self.bucket_name}/{s3_key}"]

        if self.aws_profile:
            cmd.extend(["--profile", self.aws_profile])

        return cmd

    def _upload_file(self, local_file):
        """
        Upload a single file to S3

        Args:
            local_file (str): Path to local file

        Returns:
            tuple: (success: bool, local_file: str, error_message: str or None)
        """
        try:
            # Convert to Path object for easier manipulation
            file_path = Path(local_file)

            # Create S3 key
            if self.s3_prefix:
                s3_key = f"{self.s3_prefix}/{file_path.name}"
            else:
                s3_key = file_path.name

            # Build and execute AWS CLI command
            cmd = self._build_aws_command(str(file_path), s3_key)

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=3600,  # 1 hour timeout per file
                check=False,
            )

            if result.returncode == 0:
                with self.lock:
                    self.uploaded_count += 1
                    progress = (
                        (self.uploaded_count + self.failed_count)
                        / self.total_files
                        * 100
                    )
                    print(
                        f"✓ [{progress:5.1f}%] Uploaded: {file_path.name} -> s3://{self.bucket_name}/{s3_key}"
                    )
                return True, str(file_path), None
            else:
                error_msg = result.stderr.strip() or result.stdout.strip()
                with self.lock:
                    self.failed_count += 1
                    progress = (
                        (self.uploaded_count + self.failed_count)
                        / self.total_files
                        * 100
                    )
                    print(
                        f"❌ [{progress:5.1f}%] Failed: {file_path.name} - {error_msg}"
                    )
                return False, str(file_path), error_msg

        except subprocess.TimeoutExpired:
            error_msg = "Upload timeout (5 minutes exceeded)"
            with self.lock:
                self.failed_count += 1
                progress = (
                    (self.uploaded_count + self.failed_count) / self.total_files * 100
                )
                print(f"❌ [{progress:5.1f}%] Timeout: {local_file} - {error_msg}")
            return False, local_file, error_msg
        except Exception as e:
            error_msg = str(e)
            with self.lock:
                self.failed_count += 1
                progress = (
                    (self.uploaded_count + self.failed_count) / self.total_files * 100
                )
                print(f"❌ [{progress:5.1f}%] Error: {local_file} - {error_msg}")
            return False, local_file, error_msg

    def upload_files(self, file_list):
        """
        Upload multiple files concurrently

        Args:
            file_list (list): List of file paths to upload

        Returns:
            dict: Summary of upload results
        """
        # Filter out non-existent files
        valid_files = []
        for file_path in file_list:
            if os.path.isfile(file_path):
                valid_files.append(file_path)
            else:
                print(f"⚠️  Warning: File not found, skipping: {file_path}")

        if not valid_files:
            print("❌ No valid files to upload")
            return {"success": 0, "failed": 0, "errors": []}

        self.total_files = len(valid_files)
        self.uploaded_count = 0
        self.failed_count = 0

        print(
            f"🚀 Starting upload of {self.total_files} files to s3://{self.bucket_name}/"
        )
        print(f"📊 Using {self.max_workers} concurrent workers")
        if self.s3_prefix:
            print(f"📁 S3 prefix: {self.s3_prefix}/")
        print("-" * 60)

        start_time = time.time()
        errors = []

        # Upload files concurrently
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all upload tasks
            future_to_file = {
                executor.submit(self._upload_file, file_path): file_path
                for file_path in valid_files
            }

            # Process completed uploads
            for future in as_completed(future_to_file):
                success, file_path, error_msg = future.result()
                if not success:
                    errors.append({"file": file_path, "error": error_msg})

        elapsed_time = time.time() - start_time

        # Print summary
        print("-" * 60)
        print("📈 Upload Summary:")
        print(f"   ✓ Successful: {self.uploaded_count}")
        print(f"   ❌ Failed: {self.failed_count}")
        print(f"   ⏱️  Total time: {elapsed_time:.2f} seconds")

        if self.uploaded_count > 0:
            avg_time = elapsed_time / self.uploaded_count
            print(f"   📊 Average time per file: {avg_time:.2f} seconds")

        return {
            "success": self.uploaded_count,
            "failed": self.failed_count,
            "errors": errors,
            "total_time": elapsed_time,
        }


def get_files_from_directory(directory, pattern="*", recursive=False):
    """Get list of files from directory with optional pattern matching"""
    dir_path = Path(directory)

    if not dir_path.exists():
        print(f"❌ Directory not found: {directory}")
        return []

    if recursive:
        files = list(dir_path.rglob(pattern))
    else:
        files = list(dir_path.glob(pattern))

    # Filter only files (not directories)
    files = [f for f in files if f.is_file()]
    return [str(f) for f in files]


def expand_file_patterns(file_patterns):
    """Expand file patterns/globs into actual file paths"""
    all_files = []

    for pattern in file_patterns:
        if "*" in pattern or "?" in pattern:
            # Handle glob patterns
            matched_files = glob.glob(pattern)
            if matched_files:
                all_files.extend(matched_files)
            else:
                print(f"⚠️  Warning: No files matched pattern: {pattern}")
        else:
            # Regular file path
            all_files.append(pattern)

    return all_files


def main():
    parser = argparse.ArgumentParser(
        description="Upload multiple files to S3 bucket concurrently using AWS CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Upload specific files
  python s3_concurrent_uploader.py --bucket my-bucket --files file1.txt file2.txt file3.txt

  # Upload all files from directory
  python s3_concurrent_uploader.py --bucket my-bucket --directory /path/to/files

  # Upload with pattern matching
  python s3_concurrent_uploader.py --bucket my-bucket --files *.txt *.jpg

  # Upload with custom settings
  python s3_concurrent_uploader.py --bucket my-bucket --files *.* --workers 10 --prefix uploads/

  # Upload recursively from directory
  python s3_concurrent_uploader.py --bucket my-bucket --directory /path/to/files --recursive
        """,
    )

    parser.add_argument("--bucket", "-b", required=True, help="S3 bucket name")

    parser.add_argument(
        "--files",
        "-f",
        nargs="+",
        help="Files to upload (supports glob patterns like *.txt)",
    )

    parser.add_argument(
        "--directory", "-d", help="Directory containing files to upload"
    )

    parser.add_argument(
        "--pattern",
        "-p",
        default="*",
        help="File pattern when using --directory (default: *)",
    )

    parser.add_argument(
        "--recursive", "-r", action="store_true", help="Search directory recursively"
    )

    parser.add_argument(
        "--workers",
        "-w",
        type=int,
        default=5,
        help="Number of concurrent upload workers (default: 5)",
    )

    parser.add_argument(
        "--prefix", default="", help="S3 prefix/folder for uploaded files"
    )

    parser.add_argument("--profile", help="AWS profile to use")

    args = parser.parse_args()

    # Validate arguments
    if not args.files and not args.directory:
        print("❌ Error: Either --files or --directory must be specified")
        parser.print_help()
        sys.exit(1)

    # Get list of files to upload
    files_to_upload = []

    if args.files:
        files_to_upload.extend(expand_file_patterns(args.files))

    if args.directory:
        dir_files = get_files_from_directory(
            args.directory, args.pattern, args.recursive
        )
        files_to_upload.extend(dir_files)

    # Remove duplicates while preserving order
    files_to_upload = list(dict.fromkeys(files_to_upload))

    if not files_to_upload:
        print("❌ No files found to upload")
        sys.exit(1)

    # Create uploader and start upload
    uploader = S3Uploader(
        bucket_name=args.bucket,
        max_workers=args.workers,
        s3_prefix=args.prefix,
        aws_profile=args.profile,
    )

    try:
        results = uploader.upload_files(files_to_upload)

        # Exit with appropriate code
        if results["failed"] > 0:
            print("\n⚠️  Some uploads failed. Check the errors above.")
            sys.exit(1)
        else:
            print(f"\n🎉 All {results['success']} files uploaded successfully!")
            sys.exit(0)

    except KeyboardInterrupt:
        print("\n\n⚠️  Upload interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
