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
    """Create a backup of current DAGs before sync"""
    backup_dir = f"/tmp/dags_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    if os.path.exists(DAGS_DIR):
        shutil.copytree(
            DAGS_DIR, backup_dir, ignore=shutil.ignore_patterns("__pycache__", "*.pyc")
        )
        print(f"💾 Created backup of current DAGs at: {backup_dir}")
    else:
        print(f"📁 DAGs directory doesn't exist: {DAGS_DIR}")

    return backup_dir


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

    print("🔄 Forcing DAG reprocessing to update versions...")

    # Method 1: Update modification time of all Python files
    dag_files = glob.glob(f"{DAGS_DIR}/*.py")
    current_time = time.time()

    for dag_file in dag_files:
        # Update both access and modification time to current time
        os.utime(dag_file, (current_time, current_time))
        print(f"📝 Updated timestamp for: {os.path.basename(dag_file)}")

    # Method 2: Create a temporary file to trigger directory change
    temp_file = f"{DAGS_DIR}/.dag_refresh_trigger"
    with open(temp_file, "w") as f:
        f.write(f"DAG refresh triggered at {time.ctime()}")

    # Remove the temp file immediately
    time.sleep(1)
    if os.path.exists(temp_file):
        os.remove(temp_file)

    print("✅ DAG refresh triggers completed!")
    print(
        "⏰ Airflow should detect changes within 5 minutes (or immediately if dag processor is active)"
    )

    return "DAG refresh completed successfully"


force_dag_refresh_task = PythonOperator(
    task_id="force_dag_refresh",
    python_callable=force_dag_refresh,
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
    >> validate_task
    >> force_dag_refresh_task
    >> final_cleanup_task
    >> end_task
)
