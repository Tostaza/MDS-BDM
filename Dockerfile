FROM apache/spark:3.4.1

USER root

COPY ./jars/hadoop-aws-3.3.4.jar /opt/spark/jars/
COPY ./jars/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/

RUN pip install --no-cache-dir jupyter pyspark boto3

USER root