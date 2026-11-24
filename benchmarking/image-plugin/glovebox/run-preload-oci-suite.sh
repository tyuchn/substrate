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
PROJECT_ID="${PROJECT_ID:?set PROJECT_ID}"
RAPID_BUCKET="${RAPID_BUCKET:-ate-bench-rapid-${PROJECT_ID}}"
GCS_ENDPOINT="${GCS_ENDPOINT:-google-c2p:///storage.googleapis.com}"

echo "=== [1/3] Installing crane and nydus-static v2.3.0 on Ubuntu node ==="
if [ ! -x /usr/local/bin/crane ]; then
  curl -fsSL "https://github.com/google/go-containerregistry/releases/download/v0.20.3/go-containerregistry_Linux_x86_64.tar.gz" |
    sudo tar -xz -C /usr/local/bin crane
  sudo chmod 755 /usr/local/bin/crane
fi

if [ ! -x /usr/local/bin/nydusify ]; then
  tmp_nyd="$(mktemp -d)"
  curl -fsSL -o "${tmp_nyd}/nydus.tgz" \
    "https://github.com/dragonflyoss/nydus/releases/download/v2.3.0/nydus-static-v2.3.0-linux-amd64.tgz"
  echo "c6bed2e26151ba07236df995553f568b9f12ce4a294df7e9373a30c05381a1a1  ${tmp_nyd}/nydus.tgz" | sha256sum -c --quiet
  tar -xzf "${tmp_nyd}/nydus.tgz" -C "${tmp_nyd}"
  sudo cp -f "${tmp_nyd}/nydus-static/nydusify" "${tmp_nyd}/nydus-static/nydus-image" "${tmp_nyd}/nydus-static/nydusd" /usr/local/bin/
  sudo chmod 755 /usr/local/bin/nydusify /usr/local/bin/nydus-image /usr/local/bin/nydusd
  rm -rf "${tmp_nyd}"
fi

sudo mkdir -p /mnt/work-tmpfs
if ! mountpoint -q /mnt/work-tmpfs; then
  sudo mount -t tmpfs -o size=64G,mode=0755 tmpfs /mnt/work-tmpfs
fi

echo "=== [2/3] Ensuring vduse-blk-zbd-daemon is configured for 128 x 256 MiB (32 GiB) ZBDs ==="
sudo modprobe -a vduse vdpa virtio_vdpa virtio_blk btrfs
sudo tee /etc/systemd/system/vduse-blk-zbd-daemon.service >/dev/null <<EOF
[Unit]
Description=Glovebox VDUSE ZBD DaemonPool (${RAPID_BUCKET})
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=root
Group=root
Environment=GRPC_DEFAULT_SSL_ROOTS_FILE_PATH=/etc/ssl/certs/ca-certificates.crt
ExecStartPre=-/bin/rm -f /tmp/vduse_zbd_pool.sock
ExecStart=/usr/local/bin/vduse-blk-zbd-daemon \
  --bucket=${RAPID_BUCKET} \
  --gcs_endpoint=${GCS_ENDPOINT} \
  --zone_size_mb=256 \
  --num_zones=128 \
  --pool_size=16 \
  --enforce_kernel_ipv6_support=false
LimitNOFILE=262144
LimitNPROC=65536
LimitMEMLOCK=infinity
TimeoutStopSec=15
Restart=on-failure
RestartSec=2

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl restart vduse-blk-zbd-daemon.service
for _ in $(seq 1 100); do
  if [ -S /tmp/vduse_zbd_pool.sock ] && sudo /usr/local/bin/pool-ctl --vduse PING >/dev/null 2>&1; then
    break
  fi
  sleep 0.1
done
sudo /usr/local/bin/pool-ctl --vduse PING

echo "=== [3/3] Running 16-image OCI + Golden Glovebox ZBD + Nydus RAFS v6 pipeline ==="
sudo PROJECT_ID="${PROJECT_ID}" RAPID_BUCKET="${RAPID_BUCKET}" python3 "${SCRIPT_DIR}/preload_oci_suite.py"
