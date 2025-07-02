# spark/Dockerfile

# This image is intended for local development only.
# Contains known CVEs; not recommended for production without security hardening.

FROM bitnami/spark:3.5.0

# Install pip and Python dependencies
USER root

# Copy requirements file
COPY requirements.txt /tmp/requirements.txt

RUN apt-get update && \
    apt-get install -y python3-pip && \
    pip3 install --upgrade pip && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip3 install -r /tmp/requirements.txt

# Install connector for Cassandra
ENV SPARK_EXTRA_CLASSPATH=/opt/bitnami/spark/jars/spark-cassandra-connector-assembly_2.12-3.5.0.jar
ADD https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector-assembly_2.12/3.5.0/spark-cassandra-connector-assembly_2.12-3.5.0.jar /opt/bitnami/spark/jars/

# Set workdir to app folder
WORKDIR /app
