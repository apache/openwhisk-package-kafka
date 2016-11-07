# Dockerfile for docker skeleton (useful for running blackbox binaries, scripts, or python actions).
FROM python:2.7.12-alpine

# Upgrade and install basic Python dependencies
RUN apk add --no-cache bash \
    && apk add --no-cache --virtual .build-deps \
        bzip2-dev \
        gcc \
        libc-dev \
        linux-headers \
    && pip install --no-cache-dir gevent==1.1.2 flask==0.11.1 kafka_python==1.3.1 requests==2.10.0 cloudant==2.1.0 \
      psutil==5.0.0

ENV PORT 5000

RUN mkdir -p /KafkaFeedProvider
ADD provider/*.py /KafkaFeedProvider/

CMD ["/bin/bash", "-c", "cd KafkaFeedProvider && python -u app.py"]
