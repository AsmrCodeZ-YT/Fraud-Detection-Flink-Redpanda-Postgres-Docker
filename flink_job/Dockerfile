FROM flink:1.18.1-scala_2.12-java11

# Install Python 3 + pip
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends \
        python3 python3-pip python3-dev wget curl && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*




ENV PIP_INDEX_URL=https://mirror-pypi.runflare.com/simple
ENV PIP_TRUSTED_HOST=mirror-pypi.runflare.com

# RUN pip3 install --no-cache-dir --upgrade pip setuptools wheel pycodestyle apache-flink==1.20.3


# Install PyFlink (must match Flink cluster version)
RUN pip3 install --no-cache-dir --upgrade pip setuptools wheel pycodestyle apache-flink==1.18.1 requests 

# ─── Download connector JARs into Flink lib directory ────────────────────────
ENV FLINK_LIB=/opt/flink/lib
ENV MVN=https://repo1.maven.org/maven2

# Kafka SQL connector (fat-jar — bundles kafka-clients)
RUN wget -q --retry-connrefused --waitretry=5 --tries=5 \
    -O ${FLINK_LIB}/flink-sql-connector-kafka-3.1.0-1.18.jar \
    ${MVN}/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar

# JDBC connector
RUN wget -q --retry-connrefused --waitretry=5 --tries=5 \
    -O ${FLINK_LIB}/flink-connector-jdbc-3.1.2-1.18.jar \
    ${MVN}/org/apache/flink/flink-connector-jdbc/3.1.2-1.18/flink-connector-jdbc-3.1.2-1.18.jar

# PostgreSQL JDBC driver
RUN wget -q --retry-connrefused --waitretry=5 --tries=5 \
    -O ${FLINK_LIB}/postgresql-42.7.3.jar \
    ${MVN}/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar

# Prometheus metrics reporter
RUN wget -q --retry-connrefused --waitretry=5 --tries=5 \
    -O ${FLINK_LIB}/flink-metrics-prometheus-1.18.1.jar \
    ${MVN}/org/apache/flink/flink-metrics-prometheus/1.18.1/flink-metrics-prometheus-1.18.1.jar

WORKDIR /opt/flink/usrlib
