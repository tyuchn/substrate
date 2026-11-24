#!/usr/bin/env bash
# Copyright 2026 Google LLC
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

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

sudo mkdir -p /mnt/work-tmpfs
if ! mountpoint -q /mnt/work-tmpfs; then
  sudo mount -t tmpfs -o size=64G tmpfs /mnt/work-tmpfs
else
  sudo mount -o remount,size=64G /mnt/work-tmpfs
fi

sudo modprobe -a vduse vdpa virtio_vdpa virtio_blk erofs
sudo AR_HOST="${AR_HOST:-us-central1-docker.pkg.dev}" python3 "${SCRIPT_DIR}/preload_oci_erofs.py"
