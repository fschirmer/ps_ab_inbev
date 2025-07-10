# Dockerfile
FROM apache/airflow:3.0.2-python3.11

# Switch to root to install system dependencies (like Java)
USER root

# 1. Instalar Java (JRE) e garantir que o APT esteja atualizado
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# 2. Configurar JAVA_HOME (isso pode ser feito como root)
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# Switch back to airflow user for pip installations and other operations
# that should be done as the Airflow user.
USER airflow

# 3. Instalar/Atualizar pip e dependências de requirements.txt
ADD requirements.txt .
RUN set -eux; \
    pip install --no-cache-dir --upgrade pip; \
    pip install --no-cache-dir -r requirements.txt