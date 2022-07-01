
# Base airflow image
FROM apache/airflow:2.2.3

ENV AIRFLOW_HOME=/opt/airflow

# Get required Python dependencies
COPY airflow/requirements.txt .
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Obtain root access for installing required packages
USER root
RUN apt-get update -qq

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

# Add GCS environment variables
ARG CLOUD_SDK_VERSION=322.0.0
ENV GCLOUD_HOME=/home/google-cloud-sdk

ENV PATH="${GCLOUD_HOME}/bin/:${PATH}"

# Get cloud SDK
RUN DOWNLOAD_URL="https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz" \
    && TMP_DIR="$(mktemp -d)" \
    && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/google-cloud-sdk.tar.gz" \
    && mkdir -p "${GCLOUD_HOME}" \
    && tar xzf "${TMP_DIR}/google-cloud-sdk.tar.gz" -C "${GCLOUD_HOME}" --strip-components=1 \
    && "${GCLOUD_HOME}/install.sh" \
       --bash-completion=false \
       --path-update=false \
       --usage-reporting=false \
       --quiet \
    && rm -rf "${TMP_DIR}" \
    && gcloud --version

WORKDIR $AIRFLOW_HOME

# Add entry point
COPY airflow/scripts scripts
RUN chmod +x scripts

# Switch to non-root user
USER $AIRFLOW_UID

# Add pyspark script
ENV SPARK_HOME=/opt/spark/
