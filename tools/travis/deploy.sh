#!/bin/bash

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

set -eu

dockerhub_image_prefix="$1"
dockerhub_image_name="$2"
dockerhub_image_tag="$3"
dockerhub_image="${dockerhub_image_prefix}/${dockerhub_image_name}:${dockerhub_image_tag}"

docker login -u "${DOCKER_USER}" -p "${DOCKER_PASSWORD}"

echo docker build . --tag ${dockerhub_image}
docker build . --tag ${dockerhub_image}

echo docker push ${dockerhub_image}
docker push ${dockerhub_image}

# if image tag is nightly, also push a tag with the hash commit
if [ ${dockerhub_image_tag} == "nightly" ]; then
  short_commit=`git rev-parse --short HEAD`
  dockerhub_githash_image="${dockerhub_image_prefix}/${dockerhub_image_name}:${short_commit}"

  echo docker tag ${dockerhub_image} ${dockerhub_githash_image}
  docker tag ${dockerhub_image} ${dockerhub_githash_image}

  echo docker push ${dockerhub_githash_image}
  docker push ${dockerhub_githash_image}
fi
