# Dockerfile for docker skeleton (useful for running blackbox binaries, scripts, or python actions).
FROM python:2.7.12-alpine

# Upgrade and install basic Python dependencies
RUN apk add --no-cache bash \
    && pip install --no-cache-dir flask==0.11.1 kafka_python==1.3.1 requests==2.10.0 cloudant==2.1.0

ENV FLASK_PROXY_PORT 8080

RUN mkdir -p /KafkaFeedProvider
ADD provider/*.py /KafkaFeedProvider/

CMD ["/bin/bash", "-c", "cd KafkaFeedProvider && python -u app.py"]
