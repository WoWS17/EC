FROM docker.elastic.co/logstash/logstash:8.7.0

USER root

# Install Python and pip
RUN apt-get update && apt-get install -y python3 python3-pip

# Install spotipy library
RUN pip3 install striprtf

# Switch back to the default Logstash user
USER logstash

# Copy configuration and script files
COPY pipeline/logstash.conf /usr/share/logstash/pipeline/logstash.conf
COPY ingest.py /ingest.py
COPY ./eco-inguine /eco-inguine
