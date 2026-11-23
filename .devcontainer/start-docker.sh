#!/bin/sh
# Start the Docker engine inside the dev container, then run the container's command.
# The engine starts empty: no lab images are pulled or started here.
set -e
if ! docker info >/dev/null 2>&1; then
    /usr/local/bin/dind dockerd >/var/log/dockerd.log 2>&1 &
fi
exec "$@"
