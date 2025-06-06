#!/usr/bin/env python3
"""
S3 Concurrent File Downloader

This script downloads files from an S3 bucket concurrently using AWS CLI.
It provides progress tracking, error handling, and configurable concurrency levels.

Requirements:
- AWS CLI installed and configured
- Python 3.6+

Usage:
    python s3_concurrent_downloader.py --bucket my-bucket --output ./downloads
    python s3_concurrent_downloader.py --bucket my-bucket --prefix uploads/ --output ./downloads
    python s3_concurrent_downloader.py --bucket my-bucket --pattern "*.txt" --workers 10
"""

import argparse
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path


class S3Downloader:
    def __init__(
        self, bucket_name, max_workers=5, output_dir="./downloads", aws_profile=None
    ):
        """
        Initialize S3 Downloader

        Args:
            bucket_name (str): Name of the S3 bucket
            max_workers (int): Maximum number of concurrent downloads
            output_dir (str): Local directory to save downloaded files
            aws_profile (str): AWS profile to use (optional)
        """
        self.bucket_name = bucket_name
        self.max_workers = max_workers
        self.output_dir = Path(output_dir)
        self.aws_profile = aws_profile

        self.lock = threading.Lock()
        self.downloaded_count = 0
        self.failed_count = 0
        self.total_files = 0

        # Create output directory if it doesn't exist
        self.output_dir.mkdir(parents=True, exist_ok=True)

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

    def _build_aws_list_command(self, s3_prefix=""):
        """Build AWS CLI command to list S3 objects"""
        if s3_prefix:
            s3_path = f"s3://{self.bucket_name}/{s3_prefix.rstrip('/')}/"
        else:
            s3_path = f"s3://{self.bucket_name}/"

        cmd = ["aws", "s3", "ls", s3_path, "--recursive"]

        if self.aws_profile:
            cmd.extend(["--profile", self.aws_profile])

        return cmd

    def _build_aws_download_command(self, s3_key, local_file):
        """Build AWS CLI command for file download"""
        s3_path = f"s3://{self.bucket_name}/{s3_key}"
        cmd = ["aws", "s3", "cp", s3_path, str(local_file)]

        if self.aws_profile:
            cmd.extend(["--profile", self.aws_profile])

        return cmd

    def _list_s3_files(self, s3_prefix="", pattern=None):
        """
        List files in S3 bucket

        Args:
            s3_prefix (str): S3 prefix/folder to search in
            pattern (str): File pattern to match (e.g., "*.txt")

        Returns:
            list: List of S3 object keys
        """
        try:
            cmd = self._build_aws_list_command(s3_prefix)
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=1800, check=False
            )

            if result.returncode != 0:
                error_msg = result.stderr.strip() or result.stdout.strip()
                print(f"❌ Error listing S3 objects: {error_msg}")
                return []

            # Parse AWS CLI output
            s3_files = []
            for line in result.stdout.strip().split("\n"):
                if line.strip():
                    # AWS S3 ls output format: "2023-12-01 10:30:45     123456 path/to/file.txt"
                    parts = line.split()
                    if len(parts) >= 4:
                        # Get the file path (everything after size)
                        file_key = " ".join(parts[3:])

                        # Apply pattern filter if specified
                        if pattern:
                            import fnmatch

                            if not fnmatch.fnmatch(Path(file_key).name, pattern):
                                continue

                        s3_files.append(file_key)

            return s3_files

        except Exception as e:
            print(f"❌ Error listing S3 files: {e}")
            return []

    def _download_file(self, s3_key):
        """
        Download a single file from S3

        Args:
            s3_key (str): S3 object key

        Returns:
            tuple: (success: bool, s3_key: str, error_message: str or None)
        """
        try:
            # Create local file path
            local_file = self.output_dir / Path(s3_key).name

            # Create directory if it doesn't exist
            local_file.parent.mkdir(parents=True, exist_ok=True)

            # Build and execute AWS CLI command
            cmd = self._build_aws_download_command(s3_key, local_file)

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=3600,  # 1 hour timeout per file
                check=False,
            )

            if result.returncode == 0:
                with self.lock:
                    self.downloaded_count += 1
                    progress = (
                        (self.downloaded_count + self.failed_count)
                        / self.total_files
                        * 100
                    )
                    print(f"✓ [{progress:5.1f}%] Downloaded: {s3_key} -> {local_file}")
                return True, s3_key, None
            else:
                error_msg = result.stderr.strip() or result.stdout.strip()
                with self.lock:
                    self.failed_count += 1
                    progress = (
                        (self.downloaded_count + self.failed_count)
                        / self.total_files
                        * 100
                    )
                    print(f"❌ [{progress:5.1f}%] Failed: {s3_key} - {error_msg}")
                return False, s3_key, error_msg

        except subprocess.TimeoutExpired:
            error_msg = "Download timeout (1 hour exceeded)"
            with self.lock:
                self.failed_count += 1
                progress = (
                    (self.downloaded_count + self.failed_count) / self.total_files * 100
                )
                print(f"❌ [{progress:5.1f}%] Timeout: {s3_key} - {error_msg}")
            return False, s3_key, error_msg
        except Exception as e:
            error_msg = str(e)
            with self.lock:
                self.failed_count += 1
                progress = (
                    (self.downloaded_count + self.failed_count) / self.total_files * 100
                )
                print(f"❌ [{progress:5.1f}%] Error: {s3_key} - {error_msg}")
            return False, s3_key, error_msg

    def download_files(self, s3_prefix="", pattern=None):
        """
        Download files from S3 bucket concurrently

        Args:
            s3_prefix (str): S3 prefix/folder to download from
            pattern (str): File pattern to match (e.g., "*.txt")

        Returns:
            dict: Summary of download results
        """
        print(f"🔍 Listing files in s3://{self.bucket_name}/{s3_prefix or ''}")

        # Get list of files to download
        s3_files = self._list_s3_files(s3_prefix, pattern)

        if not s3_files:
            print("❌ No files found to download")
            return {"success": 0, "failed": 0, "errors": []}

        self.total_files = len(s3_files)
        self.downloaded_count = 0
        self.failed_count = 0

        print(
            f"🚀 Starting download of {self.total_files} files from s3://{self.bucket_name}/"
        )
        print(f"📊 Using {self.max_workers} concurrent workers")
        print(f"📁 Output directory: {self.output_dir}")
        if s3_prefix:
            print(f"📁 S3 prefix: {s3_prefix}/")
        if pattern:
            print(f"🔍 File pattern: {pattern}")
        print("-" * 60)

        start_time = time.time()
        errors = []

        # Download files concurrently
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all download tasks
            future_to_key = {
                executor.submit(self._download_file, s3_key): s3_key
                for s3_key in s3_files
            }

            # Process completed downloads
            for future in as_completed(future_to_key):
                success, s3_key, error_msg = future.result()
                if not success:
                    errors.append({"file": s3_key, "error": error_msg})

        elapsed_time = time.time() - start_time

        # Print summary
        print("-" * 60)
        print("📈 Download Summary:")
        print(f"   ✓ Successful: {self.downloaded_count}")
        print(f"   ❌ Failed: {self.failed_count}")
        print(f"   ⏱️  Total time: {elapsed_time:.2f} seconds")

        if self.downloaded_count > 0:
            avg_time = elapsed_time / self.downloaded_count
            print(f"   📊 Average time per file: {avg_time:.2f} seconds")

        return {
            "success": self.downloaded_count,
            "failed": self.failed_count,
            "errors": errors,
            "total_time": elapsed_time,
        }


def main():
    parser = argparse.ArgumentParser(
        description="Download files from S3 bucket concurrently using AWS CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download all files from bucket
  python s3_concurrent_downloader.py --bucket my-bucket --output ./downloads

  # Download files from specific prefix/folder
  python s3_concurrent_downloader.py --bucket my-bucket --prefix uploads/ --output ./downloads

  # Download with pattern matching
  python s3_concurrent_downloader.py --bucket my-bucket --pattern "*.txt" --output ./downloads

  # Download with custom settings
  python s3_concurrent_downloader.py --bucket my-bucket --workers 10 --output ./my-downloads

  # Download using specific AWS profile
  python s3_concurrent_downloader.py --bucket my-bucket --profile my-profile --output ./downloads
        """,
    )

    parser.add_argument("--bucket", "-b", required=True, help="S3 bucket name")

    parser.add_argument(
        "--output",
        "-o",
        default="./downloads",
        help="Output directory for downloaded files (default: ./downloads)",
    )

    parser.add_argument(
        "--prefix", "-p", default="", help="S3 prefix/folder to download from"
    )

    parser.add_argument(
        "--pattern", help="File pattern to match (e.g., '*.txt', '*.jpg')"
    )

    parser.add_argument(
        "--workers",
        "-w",
        type=int,
        default=5,
        help="Number of concurrent download workers (default: 5)",
    )

    parser.add_argument("--profile", help="AWS profile to use")

    args = parser.parse_args()

    # Create downloader and start download
    downloader = S3Downloader(
        bucket_name=args.bucket,
        max_workers=args.workers,
        output_dir=args.output,
        aws_profile=args.profile,
    )

    try:
        results = downloader.download_files(s3_prefix=args.prefix, pattern=args.pattern)

        # Exit with appropriate code
        if results["failed"] > 0:
            print("\n⚠️  Some downloads failed. Check the errors above.")
            sys.exit(1)
        else:
            print(f"\n🎉 All {results['success']} files downloaded successfully!")
            sys.exit(0)

    except KeyboardInterrupt:
        print("\n\n⚠️  Download interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
