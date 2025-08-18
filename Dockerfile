FROM apache/airflow:3.0.0-python3.11

# Switch to root user to install system packages
USER root

# Install system dependencies
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
         curl \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Copy requirements file
COPY requirements.txt /requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /requirements.txt 