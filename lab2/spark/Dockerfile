FROM apache/spark:3.5.3-java17-python3

USER root

# PostgreSQL JDBC driver
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.10/postgresql-42.7.10.jar /opt/spark/jars/
RUN chmod 644 /opt/spark/jars/postgresql-42.7.10.jar

# Install Python dependencies (clickhouse-connect for native ClickHouse writes)
RUN pip3 install --no-cache-dir pyspark==3.5.3 clickhouse-connect pandas pyarrow

ENV PATH="/opt/spark/bin:${PATH}"

CMD ["tail", "-f", "/dev/null"]
