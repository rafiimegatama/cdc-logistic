FROM flink:1.18.1-scala_2.12-java11

RUN apt-get update && \
    apt-get install -y python3 python3-pip python3-dev wget && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    pip3 install --upgrade pip && \
    pip3 install \
        apache-flink==1.18.1 \
        google-cloud-bigquery \
        google-cloud-storage \
        python-dotenv \
        fastavro && \
    wget -q https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/1.17.2/flink-connector-kafka-1.17.2.jar \
        -O /opt/flink/lib/flink-connector-kafka.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar \
        -O /opt/flink/lib/kafka-clients.jar && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY gcp_credentials.json /opt/gcp/credentials.json
RUN chmod 644 /opt/gcp/credentials.json

ENV GOOGLE_APPLICATION_CREDENTIALS=/opt/gcp/credentials.json

WORKDIR /opt/flink/jobs
