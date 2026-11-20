# Slim Spark 4.2 runtime for the Advanced Data Engineering lab.
# JRE 17 + the PySpark wheel (it ships Spark's jars) + pinned Python libraries + connector jars baked in,
# so jobs need no --packages download at run time. Keep versions in step with the Makefile.
FROM eclipse-temurin:17-jre-noble
ARG HADOOP_VERSION=3.5.0
ARG AWS_SDK_VERSION=2.35.4
ENV SPARK_HOME=/opt/spark PYSPARK_PYTHON=python3 PATH=/opt/spark/bin:$PATH PIP_BREAK_SYSTEM_PACKAGES=1
RUN apt-get update && apt-get install -y --no-install-recommends python3 python3-pip procps \
    && rm -rf /var/lib/apt/lists/*
RUN --mount=type=cache,target=/root/.cache/pip \
    python3 -m pip install pyspark==4.2.0 pandas==2.2.3 pyarrow==19.0.1 numpy==2.2.6 \
    && python3 -m pip install --no-deps delta-spark==4.4.1 \
    && ln -s "$(python3 -c 'import os, pyspark; print(os.path.dirname(pyspark.__file__))')" /opt/spark
# S3A needs only a few AWS SDK v2 modules; the 686 MB SDK bundle is excluded.
COPY bake_jars.py /tmp/bake_jars.py
RUN LAB_SPARK_PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0,io.delta:delta-spark_2.13:4.4.1,org.apache.hadoop:hadoop-aws:${HADOOP_VERSION},software.amazon.awssdk:s3:${AWS_SDK_VERSION},software.amazon.awssdk:s3-transfer-manager:${AWS_SDK_VERSION},software.amazon.awssdk:sts:${AWS_SDK_VERSION},software.amazon.awssdk:apache-client:${AWS_SDK_VERSION}" \
    LAB_SPARK_EXCLUDES="software.amazon.awssdk:bundle" \
    python3 /tmp/bake_jars.py && rm /tmp/bake_jars.py
RUN groupadd -g 185 spark && useradd -u 185 -g 185 -m -d /home/spark spark \
    && install -d -o 185 -g 185 /opt/spark/work /home/spark/.ivy2
USER spark
WORKDIR /home/spark
