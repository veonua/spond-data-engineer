# see original https://github.com/nickagel/revenue/blob/master/Dockerfile
ARG SPARK_VERSION=3.2.0
ARG HADOOP_VERSION=3.2

# Official Amazon Linux Image
FROM library/amazonlinux:latest AS builder

ARG SPARK_VERSION
ARG HADOOP_VERSION

# Change the Default Shell for 'builder'
SHELL ["/bin/bash", "-c"]

# Install Package Dependencies for Installations
RUN yum update -y && yum upgrade -y
RUN yum install -y diffutils gzip tar wget

# Install Spark
WORKDIR /opt
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz.sha512
RUN gpg --print-md sha512 spark-*-bin-hadoop*.tgz | diff - spark-*-bin-hadoop*.tgz.sha512
RUN tar fxz spark-*-bin-hadoop*.tgz
RUN rm      spark-*-bin-hadoop*.tgz*

# Copy Build Artifact
FROM library/amazonlinux:latest
COPY --from=builder /opt /opt

ARG SPARK_VERSION
ARG HADOOP_VERSION

RUN dnf install -y java-1.8.0-amazon-corretto
RUN dnf install -y python3.11 python3-pip

# Configure Environment Variables
ENV SPARK_HOME=/opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} \
    JAVA_HOME=/usr/lib/jvm/jre-1.8.0-openjdk \
    PATH=/opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}/bin:/usr/lib/jvm/jre-1.8.0-openjdk/bin:${PATH} \
    PYSPARK_PYTHON=/usr/bin/python3


RUN pip3 install setuptools 
RUN pip3 install pyspark==${SPARK_VERSION} 

COPY datalake /home/datalake

WORKDIR /home