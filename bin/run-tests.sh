#!/bin/bash

#
# Run all tests including integration tests via docker.
#
# The script places some files into target/docker-build-volumes.
# If you keep it around between builds your can greatly speed up resolving artifacts
# but on the other hand you do not test if all artifacts are still resolvable.
#
# You can configure this script by copying run-tests-config-template.sh to run-tests-config.sh
# and adjusting it to your needs.
#
# Usage:
#  ./run-tests.sh $0 [<unique build id>]
#
#  The build id is used as part of the docker image version/container names.
#


# be verbose and print all commands, better for debugging
# set -x

PROJECT_DIR=`dirname $0`/..
PROJECT_DIR=$(cd "$PROJECT_DIR"; pwd)

if [ -r "$PROJECT_DIR/bin/run-tests-config.sh" ]; then
    . "$PROJECT_DIR/bin/run-tests-config.sh"
fi

function fatal {
    echo "======== FATAL ERROR ===================================================================================" >&2
    echo "$*" >&2
    echo "========================================================================================================" >&2
    exit 2
}

BUILD_ID=${1-test}

# We preserve some files across builds if the target directory is not deleted in between.
# Especially the ivy cache can speed up builds substantially.
BUILD_VOLUME_DIR="${BUILD_VOLUME_DIR-$PROJECT_DIR/docker-volumes}"
SBT_DIR="${SBT_DIR-$BUILD_VOLUME_DIR/sbt}"
IVY2_DIR="${IVY2_DIR-$BUILD_VOLUME_DIR/ivy2}"
TARGET_DIRS="${TARGET_DIRS-$BUILD_VOLUME_DIR/targets}"
CLEANUP_TARGET_DIRS="${CLEANUP_TARGET_DIRS-true}"
CLEANUP_CONTAINERS_ON_EXIT="${CLEANUP_CONTAINERS_ON_EXIT-true}"
export MARATHON_MAX_TASKS_PER_OFFER="${MARATHON_MAX_TASKS_PER_OFFER-1}"
NO_DOCKER_CACHE="${NO_DOCKER_CACHE-true}"
MOUNT_DOCKER_SOCKET="${MOUNT_DOCKER_SOCKET-true}"

cat <<HERE
BUILD_ID $BUILD_ID
PROJECT_DIR $PROJECT_DIR

Current configuration
=====================

Change this by copying run-tests-config-template.sh to run-tests-config.sh and adjusting it to your needs.

Directories:

    BUILD_VOLUME_DIR = "$BUILD_VOLUME_DIR"
                       (the directory containing the persistent state of the build by default)
    SBT_DIR          = "$SBT_DIR"
                       (your sbt config directory)
    IVY2_DIR         = "$IVY2_DIR"
                       (your ivy2 configuration and test)
    TARGET_DIRS      = "$TARGET_DIRS"
                       (the directory containing your build results)

Docker:

    MOUNT_DOCKER_SOCKET = "$MOUNT_DOCKER_SOCKET": true or false default: true
    NO_DOCKER_CACHE = "$NO_DOCKER_CACHE" allowed: true or false default: true
                       (whether to use the docker cache when building the base image)

Cleanup:

    CLEANUP_TARGET_DIRS         = "$CLEANUP_TARGET_DIRS" allowed: true or false default: true
                                  (whether to clean the build target dirs before building)
    CLEANUP_CONTAINERS_ON_EXIT  = "$CLEANUP_CONTAINERS_ON_EXIT" allowed: true or false default: true
                                  (whether to clean/remove container images and containers on exit)

Test parameters:

    MARATHON_MAX_TASKS_PER_OFFER = "$MARATHON_MAX_TASKS_PER_OFFER"
                                   (see --max_tasks_per_offer)

Building
========

Started: $(date)
HERE

mkdir -p "$BUILD_VOLUME_DIR" "$TARGET_DIRS" || fatal "Couldn't created '$BUILD_VOLUME_DIR' '$TARGET_DIRS'" >&2

if [ "$CLEANUP_TARGET_DIRS" = "true" ]; then
    rm -rf "$TARGET_DIRS" || fatal "Couldn't clean '$TARGET_DIRS'"
fi

# Cleanup left-over containers
function cleanup {
    echo Cleaning up volumes
    docker rmi marathon-buildbase:$BUILD_ID 2>/dev/null
    docker rm -v -f marathon-itests-$BUILD_ID 2>/dev/null
}

cleanup

if [ "$CLEANUP_CONTAINERS_ON_EXIT" = "true" ]; then
    trap cleanup EXIT
fi

if ! docker build --rm=$NO_DOCKER_CACHE --no-cache=$NO_DOCKER_CACHE -t marathon-buildbase:$BUILD_ID \
    -f "$PROJECT_DIR/Dockerfile.build-base" "$PROJECT_DIR"; then
    fatal "Build for the buildbase failed" >&2
fi

docker_args=(
    run
    --rm=$CLEANUP_CONTAINERS_ON_EXIT
    --name marathon-itests-$BUILD_ID
    --memory 4g
    --memory-swap 6g
    -e MARATHON_MAX_TASKS_PER_OFFER=$MARATHON_MAX_TASKS_PER_OFFER
    -v "$SBT_DIR:/root/.sbt"
    -v "$IVY2_DIR:/root/.ivy2"
    -v "$TARGET_DIRS/main:/marathon/target"
    -v "$TARGET_DIRS/project:/marathon/project/target"
    -v "$TARGET_DIRS/mesos-simulation:/marathon/mesos-simulation/target"
    -v "/tmp/marathon-itest-marathon:/tmp/marathon-itest-marathon"
)

if [ "$MOUNT_DOCKER_SOCKET" = "true" ]; then
  docker_args+=(-v /var/run/docker.sock:/var/run/docker.sock)
fi

for docker_env in `env | grep ^DOCKER_`; do
  docker_args+=(-e "$docker_env")
done

docker_args+=(-i "marathon-buildbase:$BUILD_ID")
docker_args+=(bash -c 'sbt -Dsbt.log.format=false test integration:test && sbt -Dsbt.log.format=false "project mesos-simulation" integration:test "test:runMain mesosphere.mesos.scale.DisplayAppScalingResults"')

echo "Running docker ${docker_args[@]}"
docker "${docker_args[@]}" || fatal "build/tests failed"
