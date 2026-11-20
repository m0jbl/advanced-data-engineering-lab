# Spark runs the pandas fraud rules in both driver and worker Python processes.
FROM apache/spark:3.5.3-python3
USER root
# The Spark 3.5.3 image uses Python 3.8. Install only Delta's Python wrapper;
# the Spark distribution already provides PySpark and spark-submit resolves the JVM jars.
RUN --mount=type=cache,target=/root/.cache/pip \
    python3 -m pip install --timeout 90 --retries 5 pandas==2.0.3 pyarrow==16.1.0 numpy==1.24.4 \
    && python3 -m pip install --timeout 90 --retries 5 --no-deps delta-spark==3.1.0 \
    && install -d -o 185 -g 185 /home/spark /home/spark/.ivy2 /opt/spark/work
USER spark
