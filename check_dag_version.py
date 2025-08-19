#!/usr/bin/env python3
"""
DAG Version Checker - Utility to check current DAG versions and sync status
"""

import os
import sys
import glob
from datetime import datetime


def check_dag_versions():
    """Check versions of all DAGs in the dags directory"""
    dags_dir = "/opt/airflow/dags"

    print("🔍 DAG Version Checker")
    print("=" * 50)
    print(f"📁 Checking DAGs in: {dags_dir}")
    print(f"⏰ Check time: {datetime.now()}")
    print("=" * 50)

    # Check if dags directory exists
    if not os.path.exists(dags_dir):
        print(f"❌ DAGs directory not found: {dags_dir}")
        return

    # Find all Python files
    dag_files = glob.glob(f"{dags_dir}/*.py")

    if not dag_files:
        print("❌ No Python files found in DAGs directory")
        return

    print(f"📊 Found {len(dag_files)} Python files:")
    print()

    for dag_file in dag_files:
        filename = os.path.basename(dag_file)
        file_size = os.path.getsize(dag_file)
        mod_time = datetime.fromtimestamp(os.path.getmtime(dag_file))

        print(f"📄 {filename}")
        print(f"   📏 Size: {file_size} bytes")
        print(f"   🕒 Modified: {mod_time}")

        # Try to extract version info if it's the simple_test_dag
        if filename == "simple_test_dag.py":
            try:
                with open(dag_file, "r") as f:
                    content = f.read()

                # Extract version info
                for line in content.split("\n"):
                    if "DAG_VERSION = " in line:
                        version = line.split("=")[1].strip().strip("\"'")
                        print(f"   📋 Version: {version}")
                    elif "LAST_UPDATED = " in line:
                        updated = line.split("=")[1].strip().strip("\"'")
                        print(f"   📅 Last Updated: {updated}")
            except Exception as e:
                print(f"   ⚠️  Could not read version info: {e}")

        print()

    # Check for version tracking file
    version_file = f"{dags_dir}/.dag_version"
    if os.path.exists(version_file):
        print("📊 Git Sync Status:")
        print("-" * 30)
        try:
            with open(version_file, "r") as f:
                version_info = f.read()
            print(version_info)
        except Exception as e:
            print(f"⚠️  Could not read sync status: {e}")
    else:
        print("ℹ️  No sync status file found (.dag_version)")

    print("=" * 50)
    print("✅ Version check completed!")


if __name__ == "__main__":
    check_dag_versions()
