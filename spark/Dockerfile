FROM apache/spark:3.5.1-python3

USER root

RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    scikit-learn==1.4.2 \
    pandas==2.2.2 \
    numpy==1.26.4 \
    joblib==1.4.2 \
    redis==5.0.4 \
    boto3==1.34.69 \
    pyarrow==15.0.2

WORKDIR /opt/spark_jobs