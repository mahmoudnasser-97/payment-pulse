FROM jupyter/pyspark-notebook:spark-3.5.0

USER root

RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    scikit-learn==1.4.2 \
    pandas==2.2.2 \
    numpy==1.26.4 \
    joblib==1.4.2 \
    boto3==1.34.69 \
    pyarrow==15.0.2 \
    matplotlib==3.8.4 \
    seaborn==0.13.2 \
    imbalanced-learn==0.12.2

USER jovyan