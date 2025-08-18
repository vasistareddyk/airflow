# Airflow 3.0 Docker Setup

This project contains a complete Apache Airflow 3.0 setup using Docker Compose with PostgreSQL and Redis backends.

## 🏗️ Architecture

- **Airflow Web Server**: UI interface (port 8080)
- **Airflow Scheduler**: Manages DAG execution
- **Airflow Worker**: Executes tasks using Celery
- **PostgreSQL**: Metadata database
- **Redis**: Message broker for Celery
- **Flower**: Celery monitoring (optional, port 5555)

## 📋 Prerequisites

- Docker and Docker Compose installed
- At least 4GB RAM available
- At least 2 CPU cores recommended

## 🚀 Quick Start

1. **Clone and navigate to the project directory**

   ```bash
   cd /path/to/this/directory
   ```

2. **Start Airflow using the startup script**

   ```bash
   ./start-airflow.sh
   ```

3. **Access the Airflow Web UI**
   - URL: http://localhost:8080
   - Username: `airflow`
   - Password: `airflow`

## 📁 Project Structure

```
.
├── docker-compose.yml      # Docker Compose configuration
├── Dockerfile             # Custom Airflow image
├── requirements.txt       # Python dependencies
├── start-airflow.sh      # Startup script
├── config/
│   └── airflow.cfg       # Airflow configuration
├── dags/                 # Your DAG files go here
│   └── simple_test_dag.py # Example test DAG
├── logs/                 # Airflow logs (auto-created)
└── plugins/              # Custom plugins (auto-created)
```

## 🔧 Manual Setup (Alternative)

If you prefer to run commands manually:

1. **Set environment variables**

   ```bash
   export AIRFLOW_UID=50000
   export AIRFLOW_PROJ_DIR=.
   export _AIRFLOW_WWW_USER_USERNAME=airflow
   export _AIRFLOW_WWW_USER_PASSWORD=airflow
   ```

2. **Initialize Airflow**

   ```bash
   docker compose up airflow-init
   ```

3. **Start all services**
   ```bash
   docker compose up -d
   ```

## 🧪 Testing the Setup

The project includes a test DAG (`simple_test_dag`) that:

- ✅ Tests Python operators
- ✅ Tests Bash operators
- ✅ Tests task dependencies
- ✅ Prints system information
- ✅ Verifies Airflow environment

To test:

1. Go to the Airflow Web UI
2. Find the "simple_test_dag" DAG
3. Toggle it ON (unpause it)
4. Click "Trigger DAG" to run it manually

## 📊 Monitoring

- **Airflow Web UI**: http://localhost:8080
- **Flower (Celery monitoring)**: http://localhost:5555 (run with `docker compose --profile flower up -d`)

## 🛠️ Useful Commands

```bash
# View logs
docker compose logs -f

# View logs for specific service
docker compose logs -f airflow-webserver

# Stop all services
docker compose down

# Stop and remove volumes (clean slate)
docker compose down -v

# Restart a specific service
docker compose restart airflow-scheduler

# Execute commands in running container
docker compose exec airflow-webserver airflow dags list

# Access Airflow CLI
docker compose run --rm airflow-cli airflow dags list
```

## 🔄 Adding New DAGs

1. Place your DAG files in the `dags/` directory
2. Airflow will automatically detect them within a few seconds
3. Refresh the Web UI to see your new DAGs

## 🐛 Troubleshooting

### Common Issues

1. **Port already in use**

   ```bash
   # Check what's using port 8080
   lsof -i :8080
   # Kill the process or change the port in docker-compose.yml
   ```

2. **Permission issues**

   ```bash
   # Fix permissions
   sudo chown -R $USER:$USER logs dags plugins
   ```

3. **Services not starting**

   ```bash
   # Check service status
   docker compose ps

   # Check logs for errors
   docker compose logs airflow-init
   ```

4. **DAG not appearing**
   - Check DAG syntax: `docker compose exec airflow-webserver python /opt/airflow/dags/your_dag.py`
   - Check logs: `docker compose logs airflow-scheduler`

### Health Checks

Check if all services are healthy:

```bash
docker compose ps
```

All services should show "healthy" status.

## 🔧 Configuration

- **Airflow config**: `config/airflow.cfg`
- **Python dependencies**: `requirements.txt`
- **Docker config**: `docker-compose.yml`

## 🔒 Security Notes

This setup is configured for development/testing. For production:

- Change default passwords
- Configure proper authentication
- Use secrets management
- Enable SSL/TLS
- Configure proper networking

## 📚 Resources

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Docker Compose Documentation](https://docs.docker.com/compose/)
- [Airflow Docker Guide](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
