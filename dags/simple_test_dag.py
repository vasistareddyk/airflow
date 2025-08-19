"""
Simple Test DAG for Airflow 3.0 Setup
This DAG now focuses only on displaying git commits.
"""

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# DAG Version tracking - Update this when making changes
DAG_VERSION = "3.0.0"
LAST_UPDATED = "2025-08-19"

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
    description=f"A simplified DAG showing only git commits - v{DAG_VERSION} (Updated: {LAST_UPDATED})",
    schedule="@daily",
    catchup=False,
    tags=["git-commits", "simplified", f"v{DAG_VERSION}"],
    # Add version info to DAG params for visibility
    params={
        "dag_version": DAG_VERSION,
        "last_updated": LAST_UPDATED,
        "sync_timestamp": datetime.now().isoformat(),
    },
)


def get_git_commits():
    """Fetch and display recent git commits from the repository"""
    import subprocess
    import os

    print("🔍 FETCHING GIT COMMITS")
    print("=" * 60)
    print(f"📋 DAG VERSION (built-in): {getattr(dag, 'version', 'unknown')}")
    print(f"📅 LAST UPDATED: {LAST_UPDATED}")
    print(f"🔄 SYNC TIMESTAMP: {datetime.now().isoformat()}")
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

            # Get last 15 commits with pretty format
            result = subprocess.run(
                ["git", "log", "--oneline", "--decorate", "--graph", "-15"],
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

                # Get recent commit authors
                authors_result = subprocess.run(
                    ["git", "log", "--format=%an", "-10"],
                    capture_output=True,
                    text=True,
                    timeout=10,
                )

                if authors_result.returncode == 0:
                    authors = set(authors_result.stdout.strip().split("\n"))
                    print(f"👥 Recent authors: {', '.join(authors)}")

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
    print("🎉 Simplified DAG - Only Git Commits Task!")
    return f"Git commits fetch completed! DAG v{getattr(dag, 'version', 'unknown')}"


# SIMPLIFIED: Only one task now
git_commits_task = PythonOperator(
    task_id="show_git_commits",
    python_callable=get_git_commits,
    dag=dag,
)

# SIMPLIFIED: No task dependencies needed - just one task
git_commits_task
