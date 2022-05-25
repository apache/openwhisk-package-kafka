<!--
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
-->

# Development and Testing
## Build
Building the Kafka feed provider is a simple matter of running a `docker build` command from the root of the project. I suggest tagging the image with a memorable name, like "kafkafeedprovider":

``` sh
docker build -t kafkafeedprovider .
```

## Run
Now we need to start the provider service. This is also a simple matter of running a `docker run` command, but the details are a little tricky. The service relies on a number of environment variables in order to operate properly. They are outlined below:

### Mandatory Environment Variables
|Name|Type|Description|
|---|---|---|
|DB_URL|URL|The base URL for persistent storage (either CouchDB or Cloudant)|
|DB_USER|String|Username for your DB credentials|
|DB_PASS|String|Password for your DB credentials|

### Optional Environment Variables
|Name|Type|Description|
|---|---|---|
|INSTANCE|String|A unique identifier for this service. This is useful to differentiate log messages if you run multiple instances of the service|
|LOCAL_DEV|Boolean|If you are using a locally-deployed OpenWhisk core system, it likely has a self-signed certificate. Set `LOCAL_DEV` to `true` to allow firing triggers without checking the certificate validity. *Do not use this for production systems!*|
|PAYLOAD_LIMIT|Integer (default=900000)|The maximum payload size, in bytes, allowed during message batching. This value should be less than your OpenWhisk deployment's payload limit.|
|WORKER|String|The ID of this running instances. Useful when running multiple instances. This should be of the form `workerX`. e.g. `worker0`.
|DB_PREFIX|String|A prefix to be prepended to the default DB name|

With that in mind, starting the feed service might look something like:

```sh
docker run -e DB_URL=https://myDbHost -e DB_USER=MyDbUser -e DB_PASS=MySuperSecret -e DB_PREFIX=ow_ -p 80:5000 kafkafeedprovider
```

This example will start the provider service with the specified DB details. The container provides a number of RESTful endpoints which can be accessed on port 5000 _inside_ the container. To expose this port to the rest of the world `-p 80:5000` tells Docker to map port 80 of the host machine into port 5000 inside this new container.

After issuing the `docker run` command, you can confirm the service started correctly by inspecting the container with a `docker logs` command.

# Install Actions
The provided actions also need to be installed to your OpenWhisk deployment. We have automated this with two different shell scripts, one for Message Hub related actions, and one for generic Kafka related actions. These scripts are `installCatalog.sh` and `installKafka.sh`, respectively.

Each script requires a number of arguments which are outlined below:

|Name|Description|
|---|---|
|authKey|The OpenWhisk auth key to use when installing the actions. Typically this would be the auth key for `whisk.system`|
|edgehost|The IP address or hostname of the OpenWhisk core system.|
|dburl|The full URL (including credentials) of the CouchDB or Cloudant account used by the feed service.|
|dbprefix|A prefix to be prepended to the default DB name (ow_kafka_triggers) that will be created by the provider service.|
|apihost|The hostname or IP address of the core OpenWhisk system that will be used as the hostname for all trigger URLs. In most cases, this will be the same as `edgehost`.|

An example run might look something like:

```sh
./installKafka.sh MyOpenWhiskAuthKey 10.0.1.5 https://cloudant_user:cloudant_pw@cloudant.com staging_db_prefix 10.0.1.5
```

In addition, when running multiple instances, the following argument is required
|Name|Description|
|---|---|
|workers|An array of the IDs of the running instances with each ID of the form `workerX`. e.g. `["worker0", "worker1"]`|

When running multiple instances, an example run might look something like:

```sh
./installKafka.sh MyOpenWhiskAuthKey 10.0.1.5 https://cloudant_user:cloudant_pw@cloudant.com staging_db_prefix 10.0.1.5 "[\"worker0\", \"worker1\"]"
```

# Testing
To run the automated test suite, you can issue a Gradle command. There are some tests which talk directly to the provider service over REST, and so these tests must know the IP address and port of the running service. This is done by providing the `-Dhealth_url`, `-Dhost` and `-Dport` arguments to Gradle:

```sh
./gradlew :tests:test -Dhealth_url=http://127.0.0.1/health -Dhost=127.0.0.1 -Dport=80
```

The value of the `host` must be the IP/hostname of the Docker host running the service provider container, and the `port` must be the exposed port number. Additionally, the `OPENWHISK_HOME` environment variable must be set to the root of the local OpenWhisk directory. Ex: `export OPENWHISK_HOME=<openwhisk_directory>`.
