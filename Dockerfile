FROM python:2.7.16

RUN apt-get update && apt-get upgrade -y

# install librdkafka
ENV LIBRDKAFKA_VERSION 1.0.0
RUN git clone --depth 1 --branch v${LIBRDKAFKA_VERSION} https://github.com/edenhill/librdkafka.git librdkafka \
    && cd librdkafka \
    && ./configure \
    && make \
    && make install \
    && make clean \
    && ./configure --clean

ENV CPLUS_INCLUDE_PATH /usr/local/include
ENV LIBRARY_PATH /usr/local/lib
ENV LD_LIBRARY_PATH /usr/local/lib

RUN pip install gevent==1.1.2 flask==0.11.1 confluent-kafka==${LIBRDKAFKA_VERSION} \
    requests==2.10.0 cloudant==2.5.0 psutil==5.0.0

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

