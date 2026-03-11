#!/bin/sh
set -eu

script_dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
repo_root="$(CDPATH= cd -- "${script_dir}/../.." && pwd)"

image_repo="${INCOMING_IMAGE_REPO:-pgeraghty/incoming-demo}"
image_tag="${INCOMING_IMAGE_TAG:-latest}"
image_ref="${image_repo}:${image_tag}"

echo "Building ${image_ref} from ${repo_root}"
docker build -t "${image_ref}" "${repo_root}"

echo "Pushing ${image_ref}"
docker push "${image_ref}"

echo "Done: ${image_ref}"
