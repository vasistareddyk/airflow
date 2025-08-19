"""
Simple Test DAG for Airflow 3.0 Setup
This DAG includes basic tasks to test if Airflow is working correctly.
"""

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator

# DAG Version tracking - Update this when making changes
DAG_VERSION = "2.1.0"
LAST_UPDATED = "2025-01-18"

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    "simple_test_dag",
    default_args=default_args,
    description=f"A simple test DAG to verify Airflow setup - v{DAG_VERSION} (Updated: {LAST_UPDATED})",
    schedule="@daily",
    catchup=False,
    tags=["test", "example", "git-sync-updated", f"v{DAG_VERSION}"],
    # Add version info to DAG params for visibility
    params={
        "dag_version": DAG_VERSION,
        "last_updated": LAST_UPDATED,
        "sync_timestamp": datetime.now().isoformat(),
    },
)


def print_hello_world():
    """Simple Python function to print hello world with version"""
    import airflow

    print("🎉 Hello World from Airflow 3.0!")
    print(f"⏰ Current time: {datetime.now()}")
    print(f"🚀 Running on Airflow version: {airflow.__version__}")
    print("=" * 60)
    print(f"📋 DAG VERSION (built-in): {getattr(dag, 'version', 'unknown')}")
    print(f"📅 LAST UPDATED: {LAST_UPDATED}")
    print(f"🔄 SYNC TIMESTAMP: {datetime.now().isoformat()}")
    print("=" * 60)
    print("✨ Updated via Git Sync! This change was pulled from GitHub!")
    print("🔄 Testing DAG version update functionality!")

    # Check for version tracking file
    version_file = "/opt/airflow/dags/.dag_version"
    if os.path.exists(version_file):
        try:
            with open(version_file, "r") as f:
                version_info = f.read()
            print("📊 Git Sync Information:")
            print(version_info)
        except Exception as e:
            print(f"⚠️  Could not read version file: {e}")

    print("=" * 60)
    return f"Hello World from Airflow {airflow.__version__} - DAG v{getattr(dag, 'version', 'unknown')} completed successfully!"


def print_system_info():
    """Print system information including Airflow version"""
    import sys
    import airflow

    print("=" * 50)
    print("AIRFLOW 3.0 SYSTEM INFORMATION")
    print("=" * 50)
    print(f"🚀 Airflow version: {airflow.__version__}")
    print(f"🐍 Python version: {sys.version}")
    print(f"📁 Current working directory: {os.getcwd()}")
    print(f"🏠 Airflow home: {os.environ.get('AIRFLOW_HOME', 'Not set')}")
    print(f"📊 Executor: {os.environ.get('AIRFLOW__CORE__EXECUTOR', 'Not set')}")
    print(
        f"🗄️  Database: {os.environ.get('AIRFLOW__DATABASE__SQL_ALCHEMY_CONN', 'Not set')}"
    )
    print(f"📋 DAG VERSION (built-in): {getattr(dag, 'version', 'unknown')}")
    print(f"📅 LAST UPDATED: {LAST_UPDATED}")
    print(f"🔄 SYNC TIMESTAMP: {datetime.now().isoformat()}")
    print("=" * 50)
    print("vasista 2.1.2")
    return f"Airflow {airflow.__version__} system info completed!"


def get_git_commits():
    """Fetch and display recent git commits from the repository"""
    import subprocess
    import os

    print("🔍 FETCHING GIT COMMITS")
    print("=" * 60)

    # Check if we're in a git repository or if git bundle is available
    git_paths = [
        "/tmp/airflow/dag_bundles/git-dags/tracking_repo",  # GitDagBundle location
        "/opt/airflow/dags",  # Local dags folder
        "/tmp/airflow_repo_sync",  # Temporary sync location
    ]

    git_repo_path = None
    for path in git_paths:
        if os.path.exists(os.path.join(path, ".git")):
            git_repo_path = path
            break

    if git_repo_path:
        print(f"📂 Found git repository at: {git_repo_path}")
        try:
            # Change to git repository directory
            os.chdir(git_repo_path)

            # Get last 10 commits with pretty format
            result = subprocess.run(
                ["git", "log", "--oneline", "--decorate", "--graph", "-10"],
                capture_output=True,
                text=True,
                timeout=30,
            )

            if result.returncode == 0:
                print("📋 RECENT COMMITS:")
                print("-" * 40)
                print(result.stdout)

                # Get current branch and commit info
                branch_result = subprocess.run(
                    ["git", "branch", "--show-current"],
                    capture_output=True,
                    text=True,
                    timeout=10,
                )

                commit_result = subprocess.run(
                    ["git", "rev-parse", "HEAD"],
                    capture_output=True,
                    text=True,
                    timeout=10,
                )

                if branch_result.returncode == 0:
                    print(f"🌿 Current branch: {branch_result.stdout.strip()}")

                if commit_result.returncode == 0:
                    print(f"🔗 Current commit: {commit_result.stdout.strip()}")

                # Get commit count
                count_result = subprocess.run(
                    ["git", "rev-list", "--count", "HEAD"],
                    capture_output=True,
                    text=True,
                    timeout=10,
                )

                if count_result.returncode == 0:
                    print(f"📊 Total commits: {count_result.stdout.strip()}")

            else:
                print(f"❌ Git log failed: {result.stderr}")

        except subprocess.TimeoutExpired:
            print("⏰ Git command timed out")
        except Exception as e:
            print(f"❌ Error running git commands: {e}")
    else:
        print("❌ No git repository found in expected locations")
        print("📂 Checked paths:")
        for path in git_paths:
            exists = "✅" if os.path.exists(path) else "❌"
            git_exists = "✅" if os.path.exists(os.path.join(path, ".git")) else "❌"
            print(f"   {exists} {path} (git: {git_exists})")

    print("=" * 60)
    return "Git commits fetch completed!"


# Define tasks
start_task = EmptyOperator(
    task_id="start",
    dag=dag,
)

hello_world_task = PythonOperator(
    task_id="hello_world_python",
    python_callable=print_hello_world,
    dag=dag,
)

system_info_task = PythonOperator(
    task_id="system_info",
    python_callable=print_system_info,
    dag=dag,
)

# NEW TASK: Git commits display
git_commits_task = PythonOperator(
    task_id="show_git_commits",
    python_callable=get_git_commits,
    dag=dag,
)

version_task = BashOperator(
    task_id="show_airflow_version",
    bash_command='echo "🚀 Airflow Version Check:" && airflow version',
    dag=dag,
)

bash_task = BashOperator(
    task_id="bash_echo",
    bash_command='echo "✅ Hello from Bash task! Date: $(date)" && echo "🏃 Task running successfully!"',
    dag=dag,
)

git_sync_test_task = BashOperator(
    task_id="git_sync_verification",
    bash_command='echo "🔄 This task was added via Git Sync! DAG Version: $(date)"',
    dag=dag,
)

end_task = EmptyOperator(
    task_id="end",
    dag=dag,
)

# Define task dependencies - UPDATED to include git_commits_task
(
    start_task
    >> [hello_world_task, system_info_task, git_commits_task]
    >> version_task
    >> bash_task
    >> git_sync_test_task
    >> end_task
)
