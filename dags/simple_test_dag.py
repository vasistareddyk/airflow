"""
Simple Test DAG for Airflow 3.0 Setup
This DAG includes basic tasks to test if Airflow is working correctly.
"""

from datetime import datetime, timedelta
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
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    "simple_test_dag",
    default_args=default_args,
    description="A simple test DAG to verify Airflow setup",
    schedule="@daily",
    catchup=False,
    tags=["test", "example"],
)


def print_hello_world():
    """Simple Python function to print hello world with version"""
    import airflow

    print("🎉 Hello World from Airflow 3.0!")
    print(f"⏰ Current time: {datetime.now()}")
    print(f"🚀 Running on Airflow version: {airflow.__version__}")
    prin("testing")
    print("=" * 50)
    print("=" * 50)
    print("✨ Updated via Git Sync! This change was pulled from GitHub!")
    print("🔄 Testing DAG version update functionality!")
    return f"Hello World from Airflow {airflow.__version__} completed successfully!"


def print_system_info():
    """Print system information including Airflow version"""
    import os
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
    print("=" * 50)
    return f"Airflow {airflow.__version__} system info completed!"


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

end_task = EmptyOperator(
    task_id="end",
    dag=dag,
)

# Define task dependencies
(
    start_task
    >> [hello_world_task, system_info_task]
    >> version_task
    >> bash_task
    >> end_task
)
