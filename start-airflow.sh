#!/bin/bash

# Airflow startup script
echo "Starting Airflow 3.0 with Docker..."

# Set environment variables
export AIRFLOW_UID=50000
export AIRFLOW_PROJ_DIR=.
export _AIRFLOW_WWW_USER_USERNAME=airflow
export _AIRFLOW_WWW_USER_PASSWORD=airflow

# Initialize Airflow (first time setup)
echo "Initializing Airflow database and creating admin user..."
docker compose up airflow-init

# Start all Airflow services
echo "Starting all Airflow services..."
docker compose up -d

echo ""
echo "🚀 Airflow is starting up!"
echo "📊 Web UI will be available at: http://localhost:8080"
echo "👤 Username: airflow"
echo "🔑 Password: airflow"
echo ""
echo "To check logs: docker compose logs -f"
echo "To stop: docker compose down"
echo "To stop and remove volumes: docker compose down -v" 