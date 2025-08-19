"""
Git Sync DAG - Pull Latest DAGs from Bitbucket Repository

This DAG pulls the latest DAGs from the Bitbucket repository and syncs them to the local dags folder.

USAGE:
1. This DAG is set to manual trigger only (schedule=None)
2. To run it, go to the Airflow UI and trigger the 'git_sync_dag' manually
3. The DAG will:
   - Clean up any temporary directories
   - Create a backup of current DAGs
       - Clone the repository from: https://github.com/vasistareddyk/airflow.git
   - Sync Python files from the repository to the DAGs directory
   - Validate the sync was successful
   - Clean up temporary files

SAFETY FEATURES:
- Creates backup of existing DAGs before sync
- Validates sync results
- Uses temporary directories for cloning
- Comprehensive error handling and logging

REQUIREMENTS:
- Git must be available in the Airflow container
- Network access to GitHub
- Write permissions to /opt/airflow/dags and /tmp directories

REPOSITORY STRUCTURE:
- If the repository has a 'dags' directory, it will sync from there
- Otherwise, it will sync all Python files from the repository root
"""

from datetime import datetime, timedelta
import os
import shutil
import glob
import re
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    "git_sync_dag",
    default_args=default_args,
    description="Pull latest DAGs from Bitbucket repository",
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=["git", "sync", "deployment"],
)

# Configuration
REPO_URL = "https://github.com/vasistareddyk/airflow.git"
TEMP_DIR = "/tmp/airflow_repo_sync"
DAGS_DIR = "/opt/airflow/dags"
BACKUP_DIR = "/tmp/dags_backup_latest"


def cleanup_temp_directory():
    """Clean up temporary directory before starting"""
    if os.path.exists(TEMP_DIR):
        shutil.rmtree(TEMP_DIR)
        print(f"🧹 Cleaned up temporary directory: {TEMP_DIR}")
    else:
        print(f"📁 Temporary directory doesn't exist: {TEMP_DIR}")


def validate_sync_results():
    """Validate that the sync was successful"""
    import glob

    dag_files = glob.glob(f"{DAGS_DIR}/*.py")
    print(f"📊 Found {len(dag_files)} Python files in DAGs directory:")

    for dag_file in dag_files:
        file_size = os.path.getsize(dag_file)
        print(f"  - {os.path.basename(dag_file)} ({file_size} bytes)")

    if len(dag_files) == 0:
        raise Exception("❌ No DAG files found after sync!")

    print("✅ Sync validation completed successfully!")
    return f"Sync completed with {len(dag_files)} DAG files"


def backup_current_dags():
    """Create a backup of current DAGs before sync into a fixed path"""
    if os.path.exists(BACKUP_DIR):
        shutil.rmtree(BACKUP_DIR)
    if os.path.exists(DAGS_DIR):
        shutil.copytree(
            DAGS_DIR, BACKUP_DIR, ignore=shutil.ignore_patterns("__pycache__", "*.pyc")
        )
        print(f"💾 Created backup of current DAGs at: {BACKUP_DIR}")
    else:
        os.makedirs(BACKUP_DIR, exist_ok=True)
        print(
            f"📁 DAGs directory doesn't exist: {DAGS_DIR}. Created empty backup at {BACKUP_DIR}"
        )
    return BACKUP_DIR


# Define tasks
start_task = EmptyOperator(
    task_id="start_sync",
    dag=dag,
)

cleanup_task = PythonOperator(
    task_id="cleanup_temp_directory",
    python_callable=cleanup_temp_directory,
    dag=dag,
)

backup_task = PythonOperator(
    task_id="backup_current_dags",
    python_callable=backup_current_dags,
    dag=dag,
)

# Clone the repository
clone_repo_task = BashOperator(
    task_id="clone_repository",
    bash_command=f"""
    echo "🚀 Starting repository clone..."
    echo "📡 Repository URL: {REPO_URL}"
    echo "📁 Target directory: {TEMP_DIR}"
    
    # Clone the repository
    git clone {REPO_URL} {TEMP_DIR}
    
    echo "✅ Repository cloned successfully!"
    echo "📋 Repository contents:"
    ls -la {TEMP_DIR}/
    
    # Check if dags directory exists in the repo
    if [ -d "{TEMP_DIR}/dags" ]; then
        echo "📂 Found dags directory in repository"
        echo "📋 DAGs directory contents:"
        ls -la {TEMP_DIR}/dags/
    else
        echo "⚠️  No dags directory found in repository root"
        echo "📋 Looking for Python files in repository:"
        find {TEMP_DIR} -name "*.py" -type f
    fi
    """,
    dag=dag,
)

# Sync DAGs from repository to airflow dags directory
sync_dags_task = BashOperator(
    task_id="sync_dags",
    bash_command=f"""
    echo "🔄 Starting DAG synchronization..."
    
    # Create dags directory if it doesn't exist
    mkdir -p {DAGS_DIR}
    
    # Clear Python cache files before sync to force recompilation
    echo "🧹 Clearing Python cache files..."
    find {DAGS_DIR} -name "*.pyc" -delete
    find {DAGS_DIR} -name "__pycache__" -type d -exec rm -rf {{}} + 2>/dev/null || true
    
    # Check if there's a dags directory in the cloned repo
    if [ -d "{TEMP_DIR}/dags" ]; then
        echo "📂 Syncing from repository dags directory..."
        # Remove existing Python files first to ensure clean sync
        rm -f {DAGS_DIR}/*.py
        # Copy all Python files from repo dags directory with force overwrite
        cp -f {TEMP_DIR}/dags/*.py {DAGS_DIR}/ 2>/dev/null || echo "⚠️  No Python files found in dags directory"
    else
        echo "📂 No dags directory found, syncing all Python files from repository root..."
        # Remove existing Python files first to ensure clean sync
        rm -f {DAGS_DIR}/*.py
        # Copy all Python files from repository root with force overwrite
        find {TEMP_DIR} -name "*.py" -type f -exec cp -f {{}} {DAGS_DIR}/ \\;
    fi
    
    echo "✅ DAG synchronization completed!"
    echo "📋 Current DAGs directory contents:"
    ls -la {DAGS_DIR}/
    
    # Clear any remaining cache files after copy
    echo "🧹 Final cache cleanup..."
    find {DAGS_DIR} -name "*.pyc" -delete
    find {DAGS_DIR} -name "__pycache__" -type d -exec rm -rf {{}} + 2>/dev/null || true
    
    # Update file timestamps to ensure Airflow detects changes
    echo "🕒 Updating file timestamps to trigger DAG reprocessing..."
    touch {DAGS_DIR}/*.py
    
    # Set proper permissions
    echo "🔐 Setting proper file permissions..."
    chmod 644 {DAGS_DIR}/*.py
    
    # Force a directory timestamp update
    echo "📁 Updating directory timestamp..."
    touch {DAGS_DIR}/
    """,
    dag=dag,
)

# Validate the sync
validate_task = PythonOperator(
    task_id="validate_sync",
    python_callable=validate_sync_results,
    dag=dag,
)


# Force DAG reprocessing using Python function
def force_dag_refresh():
    """Force DAG refresh by triggering file system change detection"""
    import time
    import os
    import subprocess

    print("🔄 Forcing DAG reprocessing to update versions...")

    # Method 1: Clear all Python cache files
    print("🧹 Clearing Python cache files...")
    try:
        # Clear .pyc files
        for root, dirs, files in os.walk(DAGS_DIR):
            for file in files:
                if file.endswith(".pyc"):
                    os.remove(os.path.join(root, file))
            # Remove __pycache__ directories
            for dir_name in dirs[
                :
            ]:  # Use slice to avoid modifying list while iterating
                if dir_name == "__pycache__":
                    shutil.rmtree(os.path.join(root, dir_name))
                    dirs.remove(dir_name)
        print("✅ Cache files cleared successfully")
    except Exception as e:
        print(f"⚠️  Cache clearing warning: {e}")

    # Method 2: Update modification time of all Python files
    dag_files = glob.glob(f"{DAGS_DIR}/*.py")
    current_time = time.time()

    for dag_file in dag_files:
        # Update both access and modification time to current time
        os.utime(dag_file, (current_time, current_time))
        print(f"📝 Updated timestamp for: {os.path.basename(dag_file)}")

    # Method 3: Create a version tracking file
    version_file = f"{DAGS_DIR}/.dag_version"
    version_info = f"Last sync: {time.ctime()}\\nTimestamp: {current_time}\\nFiles synced: {len(dag_files)}"
    with open(version_file, "w") as f:
        f.write(version_info)
    print(f"📋 Created version tracking file with {len(dag_files)} DAGs")

    # Method 4: Create a temporary file to trigger directory change
    temp_file = f"{DAGS_DIR}/.dag_refresh_trigger"
    with open(temp_file, "w") as f:
        f.write(f"DAG refresh triggered at {time.ctime()}")

    # Remove the temp file immediately
    time.sleep(1)
    if os.path.exists(temp_file):
        os.remove(temp_file)

    # Method 5: Try to signal the DAG processor (if running in container)
    try:
        print("📡 Attempting to signal DAG processor...")
        # This will work in containerized environments
        subprocess.run(
            ["pkill", "-USR1", "-f", "dag-processor"], capture_output=True, timeout=5
        )
        print("✅ DAG processor signal sent")
    except Exception as e:
        print(f"ℹ️  DAG processor signal not sent (normal in some environments): {e}")

    print("✅ DAG refresh triggers completed!")
    print("⏰ Airflow should detect changes within 30 seconds")
    print(f"📊 Total DAG files processed: {len(dag_files)}")

    return f"DAG refresh completed successfully - {len(dag_files)} files processed"


def update_dag_versions():
    backup_dir = BACKUP_DIR
    dag_files = glob.glob(f"{DAGS_DIR}/*.py")
    changed = []
    for dag_file in dag_files:
        filename = os.path.basename(dag_file)
        backup_file = os.path.join(backup_dir, filename)
        if not os.path.exists(backup_file):
            print(f"New DAG file detected: {filename}")
            changed.append(filename)
        else:
            with open(dag_file, "r") as f_new, open(backup_file, "r") as f_old:
                if f_new.read() != f_old.read():
                    print(f"Change detected in: {filename}")
                    changed.append(filename)
    if changed:
        print(
            "Detected changes in the following DAGs (Airflow will bump built-in DAG versions automatically):"
        )
        for name in changed:
            print(f" - {name}")
    else:
        print("No DAG content changes detected.")
    print("Skipping file edits; relying on Airflow built-in DAG versioning.")


force_dag_refresh_task = PythonOperator(
    task_id="force_dag_refresh",
    python_callable=force_dag_refresh,
    dag=dag,
)

update_versions_task = PythonOperator(
    task_id="update_dag_versions",
    python_callable=update_dag_versions,
    dag=dag,
)

# Final cleanup
final_cleanup_task = BashOperator(
    task_id="final_cleanup",
    bash_command=f"""
    echo "🧹 Performing final cleanup..."
    rm -rf {TEMP_DIR}
    echo "✅ Temporary directory cleaned up!"
    echo "🎉 Git sync process completed successfully!"
    """,
    dag=dag,
)

end_task = EmptyOperator(
    task_id="sync_completed",
    dag=dag,
)

# Define task dependencies
(
    start_task
    >> cleanup_task
    >> backup_task
    >> clone_repo_task
    >> sync_dags_task
    >> update_versions_task
    >> validate_task
    >> force_dag_refresh_task
    >> final_cleanup_task
    >> end_task
)
