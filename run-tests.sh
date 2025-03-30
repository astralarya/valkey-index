#!/usr/bin/env bash

podman stop valkey-index-test 2>/dev/null

echo Starting valkey instance
podman run --rm --detach --name valkey-index-test --publish 127.0.0.1:6377:6379 valkey/valkey

VKPORT=6377 bun test "$@"

echo Stopping valkey instance
podman stop valkey-index-test