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

# Clears node-local OCI image cache, orphaned Glovebox OCI clones (both Btrfs and EROFS),
# and OS page cache on the Ubuntu benchmark node so every benchmark arm starts identically cold.

echo "=== Clearing /var/lib/ate/image-cache layers/manifests and orphaned Glovebox OCI mounts ==="
sudo rm -rf /var/lib/ate/image-cache/layers/* /var/lib/ate/image-cache/manifests/* /var/lib/dragonfly/* 2>/dev/null || true
sudo mkdir -p /var/lib/ate/image-cache/layers/sha256 /var/lib/ate/image-cache/manifests/sha256
if [ ! -f /var/lib/ate/image-cache/version ]; then
  printf '2\n' | sudo tee /var/lib/ate/image-cache/version >/dev/null
fi

if grep -q '/var/lib/ate/image-plugin-cache/glovebox/' /proc/mounts; then
  awk '$2 ~ "^/var/lib/ate/image-plugin-cache/glovebox/" {print $2}' /proc/mounts | sort -r | xargs -r sudo umount -l || true
fi
for ld in $(sudo losetup -a 2>/dev/null | awk -F: '/\/dev\/vd|glovebox-erofs-meta/ {print $1}'); do
  sudo losetup -d "${ld}" 2>/dev/null || true
done
sudo rm -rf /run/ate/glovebox-erofs-meta/* 2>/dev/null || true

if [ -d /sys/bus/vdpa/devices ]; then
  for dev_path in /sys/bus/vdpa/devices/vduse-os-* /sys/bus/vdpa/devices/vduse-oa-* /sys/bus/vdpa/devices/vduse-op-* /sys/bus/vdpa/devices/vduse-es-* /sys/bus/vdpa/devices/vduse-ea-*; do
    [ -e "${dev_path}" ] || continue
    dev_name="$(basename "${dev_path}")"
    sudo vdpa dev del "${dev_name}" 2>/dev/null || true
    sudo /usr/local/bin/pool-ctl --vduse STOP "device_id=${dev_name}" >/dev/null 2>&1 || true
    sudo /usr/local/bin/pool-ctl --vduse DESTROY "device_id=${dev_name}" >/dev/null 2>&1 || true
  done
fi

if [ -d /dev/vduse ]; then
  for vdev in /dev/vduse/vduse-os-* /dev/vduse/vduse-oa-* /dev/vduse/vduse-op-* /dev/vduse/vduse-es-* /dev/vduse/vduse-ea-*; do
    [ -e "${vdev}" ] || continue
    dev_name="$(basename "${vdev}")"
    sudo /usr/local/bin/pool-ctl --vduse STOP "device_id=${dev_name}" >/dev/null 2>&1 || true
    sudo /usr/local/bin/pool-ctl --vduse DESTROY "device_id=${dev_name}" >/dev/null 2>&1 || true
  done
fi

sudo sync
echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null
sleep 1

mem_avail_kb="$(awk '/^MemAvailable:/ {print $2}' /proc/meminfo)"
cached_kb="$(awk '/^Cached:/ {print $2}' /proc/meminfo)"
zbd_pid="$(pgrep -f 'vduse-blk-zbd-daemon' | head -n1 || true)"
zbd_rss_kb=0
if [ -n "${zbd_pid}" ] && [ -r "/proc/${zbd_pid}/status" ]; then
  zbd_rss_kb="$(awk '/^VmRSS:/ {print $2}' "/proc/${zbd_pid}/status")"
fi
echo "{\"phase\":\"cold_baseline\",\"mem_available_kb\":${mem_avail_kb},\"cached_kb\":${cached_kb},\"zbd_daemon_rss_kb\":${zbd_rss_kb}}"
