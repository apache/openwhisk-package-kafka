FROM buildpack-deps:xenial

# install system deps
RUN apt-get update
RUN apt-get install -y \
    python-pip \
    python-dev \
    git \
    gcc \
    make \
    zlib1g-dev \
    libsasl2-dev \
    libsasl2-modules \
    psmisc

# install librdkafka
RUN git clone --depth 1 --branch v0.9.4 https://github.com/edenhill/librdkafka.git librdkafka \
    && cd librdkafka \
    && ./configure \
    && make \
    && make install
ENV LD_LIBRARY_PATH=/usr/local/lib
RUN export LD_LIBRARY_PATH=$LD_LIBRARY_PATH \
    && ldconfig

RUN pip install gevent==1.1.2 flask==0.11.1 confluent-kafka==0.9.4 \
        requests==2.10.0 cloudant==2.1.0 psutil==5.0.0

# while I expect these will be overridden during deployment, we might as well
# set reasonable defaults
ENV PORT 5000
ENV LOCAL_DEV False
ENV GENERIC_KAFKA True

RUN mkdir -p /KafkaFeedProvider
ADD provider/*.py /KafkaFeedProvider/

# Automatically curl the health endpoint every 5 minutes.
# If the endpoint doesn't respond within 30 seconds, kill the main python process.
# As of docker 1.12, a failed healthcheck never results in the container being
# restarted. Killing the main process is a way to make the restart policy kicks in.
HEALTHCHECK --interval=5m --timeout=1m CMD curl -m 30 --fail http://localhost:5000/health || killall python

CMD ["/bin/bash", "-c", "cd KafkaFeedProvider && python -u app.py"]
