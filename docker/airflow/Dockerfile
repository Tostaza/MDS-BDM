FROM apache/airflow:3.1.2

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# S3A jars needed by the Spark driver when running in client mode (airflow-worker).
# These match the versions baked into the bdm-spark image so executor/driver jars align.
COPY jars/hadoop-aws-3.3.4.jar /tmp/
COPY jars/aws-java-sdk-bundle-1.12.262.jar /tmp/

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt && \
    cp /tmp/hadoop-aws-3.3.4.jar \
       $(python -c "import pyspark; import os; print(os.path.join(os.path.dirname(pyspark.__file__), 'jars'))") && \
    cp /tmp/aws-java-sdk-bundle-1.12.262.jar \
       $(python -c "import pyspark; import os; print(os.path.join(os.path.dirname(pyspark.__file__), 'jars'))")
