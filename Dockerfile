FROM bde2020/spark-python-template:3.1.2-hadoop3.2

RUN cd /app && pip install -r requirements.txt
ENV SPARK_APPLICATION_PYTHON_LOCATION /app/main.py
ENV SPARK_APPLICATION_ARGS "48.208667 16.373806 400 20.0 80.0 fuel"