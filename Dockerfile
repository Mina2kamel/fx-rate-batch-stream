# FROM apache/airflow:2.7.2
# USER root
# RUN apt-get update \
#   && apt-get install -y --no-install-recommends \
#          openjdk-11-jre-headless \
#   && apt-get autoremove -yqq --purge \
#   && apt-get clean \
#   && rm -rf /var/lib/apt/lists/*

# USER airflow
# ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
# RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" apache-airflow-providers-apache-spark==2.1.3

# COPY requirements.txt /opt/airflow

# WORKDIR /opt/airflow
# RUN pip install -r requirements.txt


FROM apache/airflow:2.7.2

# Switch to root to install system dependencies
USER root

# Install Java for Spark
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-11-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME for Java
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
# Add Spark to PATH
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Install Spark and Hadoop dependencies
RUN curl -fsSL https://archive.apache.org/dist/spark/spark-3.2.4/spark-3.2.4-bin-hadoop3.2.tgz | tar -xz -C /opt/ \
    && ln -s /opt/spark-3.2.4-bin-hadoop3.2 /opt/spark

# Add jars for PostgreSQL and Hadoop AWS dependencies
RUN mkdir -p $SPARK_HOME/jars && \
curl -o $SPARK_HOME/jars/postgresql-42.7.4.jar https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar && \
curl -o $SPARK_HOME/jars/hadoop-aws-3.2.2.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.2/hadoop-aws-3.2.2.jar && \
curl -o $SPARK_HOME/jars/aws-java-sdk-bundle-1.11.563.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.563/aws-java-sdk-bundle-1.11.563.jar

# Install necessary Python dependencies for Airflow and Spark integration
USER airflow
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" \
    apache-airflow-providers-apache-spark==2.1.3 \
    pyspark==3.2.4

# Add your Python dependencies
COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

WORKDIR /opt/airflow

# Set SparkSubmitOperator's default configuration
ENV PYSPARK_PYTHON=python3
