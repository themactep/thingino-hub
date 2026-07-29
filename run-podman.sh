#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
IMAGE_NAME="localhost/thinginohub:latest"
SKIP_BUILD="${SKIP_BUILD:-0}"
UI_USERNAME="${HUB_UI_USERNAME:-}"
UI_PASSWORD="${HUB_UI_PASSWORD:-}"

mkdir -p "$SCRIPT_DIR/data"

if [ "$SKIP_BUILD" != "1" ]; then
	echo "Building $IMAGE_NAME"
	podman build -t "$IMAGE_NAME" -f "$SCRIPT_DIR/Containerfile" "$SCRIPT_DIR"
elif ! podman image exists "$IMAGE_NAME"; then
  echo "Image $IMAGE_NAME not found and SKIP_BUILD=1" >&2
  exit 1
fi

exec podman run --rm --replace -d \
  --name thinginohub \
  -p 8080:8080 \
  --env HUB_STATE_PATH=/data/camera-state.yaml \
  --env HUB_UI_HOST=0.0.0.0 \
  --env HUB_UI_PORT=8080 \
  --env HUB_UI_USERNAME="$UI_USERNAME" \
  --env HUB_UI_PASSWORD="$UI_PASSWORD" \
  --mount type=bind,src="$SCRIPT_DIR/app",dst=/app/app,relabel=shared \
  --mount type=bind,src="$SCRIPT_DIR/config.yaml",dst=/config/config.yaml,relabel=private \
  --mount type=bind,src="$SCRIPT_DIR/data",dst=/data,relabel=private \
  "$IMAGE_NAME"

exit 0

