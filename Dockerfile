#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

FROM python:2.7.16

RUN apt-get update && apt-get upgrade -y

# install librdkafka
ENV LIBRDKAFKA_VERSION 1.3.0
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

RUN pip install gevent==1.1.2 flask==1.1.4 confluent-kafka==${LIBRDKAFKA_VERSION} \
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

