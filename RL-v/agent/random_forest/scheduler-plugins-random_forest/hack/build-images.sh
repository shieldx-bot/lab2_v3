#!/usr/bin/env bash

# Copyright 2023 The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -o errexit
set -o nounset
set -o pipefail

SCRIPT_ROOT=$(realpath $(dirname "${BASH_SOURCE[@]}")/..)

SCHEDULER_DIR="${SCRIPT_ROOT}"/build/scheduler
CONTROLLER_DIR="${SCRIPT_ROOT}"/build/controller

# -t is the Docker engine default
TAG_FLAG="-t"

# If docker is not present, fall back to nerdctl
# TODO: nerdctl doesn't seem to have buildx.
if ! command -v ${BUILDER} >/dev/null 2>&1 && command -v nerdctl >/dev/null 2>&1; then
  BUILDER=nerdctl
fi

# podman needs the manifest flag in order to create a single image.
if [[ "${BUILDER}" == "podman" ]]; then
  TAG_FLAG="--manifest"
fi

cd "${SCRIPT_ROOT}"

USE_BUILDX=true
if [[ -n "${DOCKER_BUILDX_CMD:-}" ]]; then
  IMAGE_BUILD_CMD="${DOCKER_BUILDX_CMD}"
  IMAGE_BUILD_SUBCMD="build"
elif ${BUILDER} buildx version >/dev/null 2>&1; then
  IMAGE_BUILD_CMD="${BUILDER} buildx"
  IMAGE_BUILD_SUBCMD="build"
else
  USE_BUILDX=false
  IMAGE_BUILD_CMD="${BUILDER}"
  IMAGE_BUILD_SUBCMD="build"
  echo "buildx is unavailable; falling back to '${BUILDER} build'."
fi

PLATFORM_ARGS=""
if [[ "${USE_BUILDX}" == "true" ]]; then
  PLATFORM_ARGS="--platform=${PLATFORMS}"
fi

EFFECTIVE_EXTRA_ARGS="${EXTRA_ARGS:-}"
FALLBACK_BUILD_ARGS=""
if [[ "${USE_BUILDX}" != "true" ]]; then
  # --load/--push are buildx-only flags; plain `docker build` already loads locally.
  EFFECTIVE_EXTRA_ARGS="${EFFECTIVE_EXTRA_ARGS//--load/}"
  EFFECTIVE_EXTRA_ARGS="${EFFECTIVE_EXTRA_ARGS//--push/}"

  LOCAL_ARCH="$(uname -m)"
  case "${LOCAL_ARCH}" in
    x86_64)
      LOCAL_ARCH="amd64"
      ;;
    aarch64)
      LOCAL_ARCH="arm64"
      ;;
  esac
  FALLBACK_BUILD_ARGS="--build-arg BUILDPLATFORM=linux/${LOCAL_ARCH} --build-arg TARGETARCH=${LOCAL_ARCH}"
fi

# use RELEASE_VERSION==v0.0.0 to tell if it's a local image build.
BLD_INSTANCE=""
if [[ "${RELEASE_VERSION}" == "v0.0.0" && "${USE_BUILDX}" == "true" ]]; then
  # Older docker/buildx combinations may not support `create --use`.
  if ! BLD_INSTANCE=$($IMAGE_BUILD_CMD create --use 2>/dev/null); then
    echo "Skipping temporary buildx instance: current builder does not support 'create --use'."
    BLD_INSTANCE=""
  fi
fi

# DOCKER_BUILDX_CMD is an env variable set in CI (valued as "/buildx-entrypoint")
# If it's set, use it; otherwise use "$BUILDER buildx"
${IMAGE_BUILD_CMD} ${IMAGE_BUILD_SUBCMD} \
  ${PLATFORM_ARGS:-} \
  -f ${SCHEDULER_DIR}/Dockerfile \
  --build-arg RELEASE_VERSION=${RELEASE_VERSION} \
  --build-arg GO_BASE_IMAGE=${GO_BASE_IMAGE} \
  --build-arg DISTROLESS_BASE_IMAGE=${DISTROLESS_BASE_IMAGE} \
  --build-arg CGO_ENABLED=0 \
  ${FALLBACK_BUILD_ARGS:-} \
  ${EFFECTIVE_EXTRA_ARGS:-}  ${TAG_FLAG:-} ${REGISTRY}/${IMAGE} .

${IMAGE_BUILD_CMD} ${IMAGE_BUILD_SUBCMD} \
  ${PLATFORM_ARGS:-} \
  -f ${CONTROLLER_DIR}/Dockerfile \
  --build-arg RELEASE_VERSION=${RELEASE_VERSION} \
  --build-arg GO_BASE_IMAGE=${GO_BASE_IMAGE} \
  --build-arg DISTROLESS_BASE_IMAGE=${DISTROLESS_BASE_IMAGE} \
  --build-arg CGO_ENABLED=0 \
  ${FALLBACK_BUILD_ARGS:-} \
  ${EFFECTIVE_EXTRA_ARGS:-} ${TAG_FLAG:-} ${REGISTRY}/${CONTROLLER_IMAGE} .

if [[ ! -z $BLD_INSTANCE ]]; then
  ${DOCKER_BUILDX_CMD:-${BUILDER} buildx} rm $BLD_INSTANCE
fi