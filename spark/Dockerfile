# spark/Dockerfile

# This image is intended for local development only.
# Contains known CVEs; not recommended for production without security hardening.

FROM bitnami/spark:3.5.0

# Install pip and Python dependencies
USER root
RUN apt-get update && \
    apt-get install -y python3-pip && \
    pip3 install --upgrade pip

# Copy and install Python dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install -r /tmp/requirements.txt

# Set workdir to app folder
WORKDIR /app
