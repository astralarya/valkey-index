#!/usr/bin/env bash

podman run --rm --name valkey-index-test --publish 127.0.0.1:"${VKPORT:-6379}":6379 valkey/valkey