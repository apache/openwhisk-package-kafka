#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more contributor
# license agreements; and to You under the Apache License, Version 2.0.

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
