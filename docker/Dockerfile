FROM apache/airflow:2.7.3-python3.10

USER root

# Configure clean Debian Bullseye repository sources and install OpenJDK-17 and procps
RUN echo "deb http://deb.debian.org/debian bullseye main" > /etc/apt/sources.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

# Install Python requirements and ensure SQLAlchemy is compatible with Airflow 2.7.3
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt \
    && pip install --no-cache-dir "SQLAlchemy<2.0.0,>=1.4.28"
