FROM apache/spark:3.5.1-python3

USER root

RUN pip install --no-cache-dir --upgrade pip

RUN pip install --no-cache-dir --timeout=120 \
    kafka-python==2.0.2 \
    scikit-learn==1.3.2 \
    pandas==1.5.3 \
    numpy==1.24.4 \
    joblib==1.3.2 \
    redis==5.0.4 \
    boto3==1.34.69 \
    pyarrow==11.0.0


COPY jars/ /opt/spark/jars/

WORKDIR /opt/spark_jobs