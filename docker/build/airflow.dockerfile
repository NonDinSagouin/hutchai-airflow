FROM apache/airflow:3.1.3

USER root

# Installer Java (requis pour PySpark)
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get clean;

ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64
RUN export JAVA_HOME

# Installer les dépendances Python et le provider Apache Spark
RUN python -m pip install pyspark==3.5.1
RUN python -m pip install pyarrow==22.0.0
RUN python -m pip install apache-airflow-providers-apache-spark==5.3.4

USER airflow
