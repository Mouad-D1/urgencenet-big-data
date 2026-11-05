# Image unique pour Master, Workers et Jupyter : le Driver et les Executors partagent
# ainsi exactement les memes versions de Spark, Python et jars.
FROM apache/spark:3.5.7-scala2.12-java17-python3-ubuntu

USER root

# Hadoop 3.3.4 est la version embarquee par Spark 3.5 : hadoop-aws doit lui correspondre.
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.7/spark-sql-kafka-0-10_2.12-3.5.7.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.7/spark-token-provider-kafka-0-10_2.12-3.5.7.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.3.2/delta-spark_2.12-3.3.2.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/io/delta/delta-storage/3.3.2/delta-storage-3.3.2.jar /opt/spark/jars/

COPY requirements.txt /tmp/requirements.txt
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

RUN chmod 644 /opt/spark/jars/*.jar \
    && python3 -m pip install --no-cache-dir -r /tmp/requirements.txt \
    && mkdir -p /home/spark \
    && chown -R spark:spark /home/spark /opt/spark/conf

ENV HOME=/home/spark \
    PATH=/opt/spark/bin:/opt/spark/sbin:/home/spark/.local/bin:$PATH \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip

USER spark
