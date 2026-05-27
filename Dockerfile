FROM flink:1.18-scala_2.12-java11

RUN apt-get update -y && \
    apt-get install -y python3 python3-pip wget && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# PyFlink must match the Flink image version exactly
RUN pip3 install --no-cache-dir apache-flink==1.18.1

# Kafka connector JAR must be on the classpath for KafkaSource to work
RUN wget -q -P /opt/flink/lib/ \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar

RUN mkdir -p /tmp/flink-output && chmod 777 /tmp/flink-output
