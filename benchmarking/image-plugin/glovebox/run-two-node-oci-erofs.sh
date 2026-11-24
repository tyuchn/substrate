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

# Evaluates 2-Node Simultaneous Right-Sized Shared EROFS ZBD Attach + Cold 1,000-File Read:
# Node 1 keeps pydata__xarray-4629 (vduse-oe-583ef721ebfc24, right-sized EROFS ZBD) mounted read-only,
# while Node 2 simultaneously creates a zero-copy CoW clone, prefetches Zone-0 metadata, mounts EROFS -o ro,
# and runs 5 cold-cache iterations reading /testbed, then scales substrate-ubuntu-pool back to 1 node.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
DATA_DIR="${DATA_DIR:-${SCRIPT_DIR}/data}"
RESULTS_ROOT="${RESULTS_ROOT:-${SCRIPT_DIR}/results}"
mkdir -p "${RESULTS_ROOT}"

if [[ -f "${ROOT}/.ate-dev-env.sh" ]]; then
  # shellcheck disable=SC1091
  source "${ROOT}/.ate-dev-env.sh"
fi

PROJECT_ID="${PROJECT_ID:?set PROJECT_ID}"
CLUSTER_NAME="${CLUSTER_NAME:?set CLUSTER_NAME}"
CLUSTER_LOCATION="${CLUSTER_LOCATION:?set CLUSTER_LOCATION}"
RAPID_BUCKET="${RAPID_BUCKET:-ate-bench-rapid-${PROJECT_ID}}"

GOLD_ID="${GOLD_ID:-$(python3 -c "import json; print(next(json.loads(l)['erofs_golden_device_id'] for l in open('${DATA_DIR}/images-16-bench-erofs.jsonl') if 'pydata__xarray-4629' in l))")}"
NUM_ZONES="$(python3 -c "import json; print(next(json.loads(l)['erofs_num_zones'] for l in open('${DATA_DIR}/images-16-bench-erofs.jsonl') if 'pydata__xarray-4629' in l))")"
META_BYTES="$(python3 -c "import json; print(next(json.loads(l)['erofs_meta_aligned_bytes'] for l in open('${DATA_DIR}/images-16-bench-erofs.jsonl') if 'pydata__xarray-4629' in l))")"
echo "=== pydata__xarray-4629 Right-Sized EROFS Golden ZBD: ${GOLD_ID} (${NUM_ZONES} zones, ${META_BYTES} meta bytes) ==="

NODE1="$(kubectl get nodes -l cloud.google.com/gke-nodepool=substrate-ubuntu-pool -o jsonpath='{.items[0].metadata.name}')"
echo "=== Node 1: ${NODE1} ==="

cleanup_pool() {
  echo "=== Scaling substrate-ubuntu-pool back to 1 node to conserve quota ==="
  gcloud container clusters resize "${CLUSTER_NAME}" \
    --node-pool=substrate-ubuntu-pool \
    --num-nodes=1 \
    --zone="${CLUSTER_LOCATION}" \
    --project="${PROJECT_ID}" \
    --quiet || true
}
trap cleanup_pool EXIT

echo "=== Scaling substrate-ubuntu-pool to 2 nodes for cross-node simultaneous EROFS ZBD test ==="
gcloud container clusters resize "${CLUSTER_NAME}" \
  --node-pool=substrate-ubuntu-pool \
  --num-nodes=2 \
  --zone="${CLUSTER_LOCATION}" \
  --project="${PROJECT_ID}" \
  --quiet

NODE2=""
for _ in $(seq 1 60); do
  for n in $(kubectl get nodes -l cloud.google.com/gke-nodepool=substrate-ubuntu-pool -o jsonpath='{.items[*].metadata.name}'); do
    if [[ "${n}" != "${NODE1}" ]]; then
      NODE2="${n}"
      break 2
    fi
  done
  sleep 3
done
echo "=== Node 2 joined: ${NODE2} ==="

kubectl label node "${NODE2}" ate.dev/glovebox=true --overwrite
kubectl taint node "${NODE2}" ate.dev/glovebox=true:NoSchedule --overwrite

TMP_BINS="$(mktemp -d)"
gcloud compute scp "${NODE1}:/usr/local/bin/vduse-blk-zbd-daemon" "${NODE1}:/usr/local/bin/pool-ctl" "${TMP_BINS}/" \
  --project="${PROJECT_ID}" --zone="${CLUSTER_LOCATION}" --quiet
gcloud compute scp "${TMP_BINS}/vduse-blk-zbd-daemon" "${TMP_BINS}/pool-ctl" "${NODE2}:/tmp/" \
  --project="${PROJECT_ID}" --zone="${CLUSTER_LOCATION}" --quiet
rm -rf "${TMP_BINS}"

gcloud compute ssh "${NODE2}" --project="${PROJECT_ID}" --zone="${CLUSTER_LOCATION}" --quiet --command="
  set -euo pipefail
  sudo mv -f /tmp/vduse-blk-zbd-daemon /usr/local/bin/vduse-blk-zbd-daemon
  sudo mv -f /tmp/pool-ctl /usr/local/bin/pool-ctl
  sudo chmod 755 /usr/local/bin/vduse-blk-zbd-daemon /usr/local/bin/pool-ctl
  sudo modprobe -a vduse vdpa virtio_vdpa virtio_blk erofs
  sudo tee /etc/systemd/system/vduse-blk-zbd-daemon.service >/dev/null <<EOF
[Unit]
Description=Glovebox VDUSE ZBD DaemonPool
After=network-online.target
Wants=network-online.target
[Service]
Type=simple
User=root
Group=root
Environment=GRPC_DEFAULT_SSL_ROOTS_FILE_PATH=/etc/ssl/certs/ca-certificates.crt
ExecStartPre=-/bin/rm -f /tmp/vduse_zbd_pool.sock
ExecStart=/usr/local/bin/vduse-blk-zbd-daemon --bucket=${RAPID_BUCKET} --gcs_endpoint=google-c2p:///storage.googleapis.com --zone_size_mb=256 --num_zones=128 --pool_size=16 --enforce_kernel_ipv6_support=false
LimitNOFILE=262144
LimitNPROC=65536
LimitMEMLOCK=infinity
Restart=on-failure
[Install]
WantedBy=multi-user.target
EOF
  sudo systemctl daemon-reload
  sudo systemctl enable --now vduse-blk-zbd-daemon.service
  for _ in \$(seq 1 100); do
    if [ -S /tmp/vduse_zbd_pool.sock ] && sudo /usr/local/bin/pool-ctl --vduse PING >/dev/null 2>&1; then
      break
    fi
    sleep 0.1
  done
  sudo /usr/local/bin/pool-ctl --vduse PING
"

gcloud compute ssh "${NODE1}" --project="${PROJECT_ID}" --zone="${CLUSTER_LOCATION}" --quiet --command="
  set -euo pipefail
  sudo mkdir -p /mnt/xnode-erofs-n1
  if ! mountpoint -q /mnt/xnode-erofs-n1; then
    sudo /usr/local/bin/pool-ctl --vduse CLONE source_device_id=${GOLD_ID} target_device_id=vduse-en1-583ef721 clone_mode=cow >/dev/null 2>&1 || true
    sudo /usr/local/bin/pool-ctl --vduse START device_index=521 device_id=vduse-en1-583ef721 parent_device_id=${GOLD_ID} zone_size_mb=256 num_zones=${NUM_ZONES} >/dev/null 2>&1 || true
    sudo vdpa dev add name vduse-en1-583ef721 mgmtdev vduse 2>/dev/null || true
    for _ in \$(seq 1 200); do
      b=\$(ls -d /sys/bus/vdpa/devices/vduse-en1-583ef721/virtio*/block/* 2>/dev/null | head -n1 || true)
      [ -n \"\${b}\" ] && break
      sleep 0.01
    done
    blk=\"/dev/\$(basename \"\${b}\")\"
    sudo mount -t erofs -o ro \"\${blk}\" /mnt/xnode-erofs-n1
  fi
  head -c 64 /mnt/xnode-erofs-n1/oci-meta.json >/dev/null
  echo 'Node 1 active EROFS RO mount confirmed on /mnt/xnode-erofs-n1'
"

cat > "${RESULTS_ROOT}/node2_erofs_trials.py" <<EOF
import glob, json, os, struct, subprocess, time, urllib.parse, urllib.request
from concurrent.futures import ThreadPoolExecutor

GOLD_ID = "${GOLD_ID}"
BUCKET = "${RAPID_BUCKET}"
NUM_ZONES = ${NUM_ZONES}
META_BYTES = ${META_BYTES}
MNT = "/mnt/xnode-erofs-n2"
BOOT_DIR = "/run/ate/glovebox-erofs-meta"
os.makedirs(MNT, exist_ok=True)
os.makedirs(BOOT_DIR, exist_ok=True)
trials = []

def get_access_token() -> str:
  req = urllib.request.Request(
      "http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token",
      headers={"Metadata-Flavor": "Google"},
  )
  with urllib.request.urlopen(req, timeout=5) as r:
    return json.loads(r.read())["access_token"]

TOKEN = get_access_token()

def stage_zone0_meta_boot(cid: str):
  ts = time.monotonic()
  obj = urllib.parse.quote(f"rapid_devices/{GOLD_ID}/zones/zone_000000.bin", safe="")
  url = f"https://storage.googleapis.com/storage/v1/b/{BUCKET}/o/{obj}?alt=media"
  req = urllib.request.Request(url, headers={
      "Authorization": f"Bearer {TOKEN}",
      "Range": f"bytes=0-{META_BYTES - 1}",
  })
  with urllib.request.urlopen(req, timeout=15) as r:
    buf = bytearray(r.read())
  extra_devs, slot_off = struct.unpack_from("<HH", buf, 1024 + 86)
  mapped_blk, = struct.unpack_from("<I", buf, slot_off * 128 + 68)
  struct.pack_into("<I", buf, 1024 + 36, mapped_blk)
  struct.pack_into("<I", buf, slot_off * 128 + 68, 0)
  boot_path = os.path.join(BOOT_DIR, f"{cid}.boot")
  with open(boot_path, "wb") as fp:
    fp.write(buf[:mapped_blk * 4096])
  return boot_path, mapped_blk * 4096, (time.monotonic() - ts) * 1000.0

for it in range(1, 6):
  cid = f"vduse-en2-{it:02d}-583ef7"
  subprocess.run(["umount", "-l", MNT], check=False)
  subprocess.run(["vdpa", "dev", "del", cid], check=False)
  subprocess.run(["/usr/local/bin/pool-ctl", "--vduse", "STOP", f"device_id={cid}"], check=False)
  subprocess.run(["/usr/local/bin/pool-ctl", "--vduse", "DESTROY", f"device_id={cid}"], check=False)
  subprocess.run(["sync"], check=True)
  with open("/proc/sys/vm/drop_caches", "w") as f:
    f.write("3\n")
  time.sleep(0.3)

  with ThreadPoolExecutor(max_workers=1) as ex:
    t0 = time.monotonic()
    stage_fut = ex.submit(stage_zone0_meta_boot, cid)
    subprocess.run(["/usr/local/bin/pool-ctl", "--vduse", "CLONE", f"source_device_id={GOLD_ID}", f"target_device_id={cid}", "clone_mode=cow"], check=True, capture_output=True)
    t1 = time.monotonic()
    subprocess.run(["/usr/local/bin/pool-ctl", "--vduse", "START", f"device_index={530+it}", f"device_id={cid}", f"parent_device_id={GOLD_ID}", "zone_size_mb=256", f"num_zones={NUM_ZONES}"], check=True, capture_output=True)
    t2 = time.monotonic()
    subprocess.run(["vdpa", "dev", "add", "name", cid, "mgmtdev", "vduse"], check=True)
    blk = ""
    bname = ""
    for _ in range(400):
      m = glob.glob(f"/sys/bus/vdpa/devices/{cid}/virtio*/block/*")
      if m and os.path.exists("/dev/" + os.path.basename(m[0])):
        bname = os.path.basename(m[0])
        blk = "/dev/" + bname
        break
      time.sleep(0.005)
    with open(f"/sys/block/{bname}/queue/read_ahead_kb", "w") as rf:
      rf.write("2048\n")
    t3 = time.monotonic()
    boot_path, patched_bytes, stage_http_ms = stage_fut.result()
    t_pre = time.monotonic()
    loop_dev = subprocess.check_output(["losetup", "--find", "--show", "--read-only", "--offset", str(patched_bytes), blk], text=True).strip()
    subprocess.run(["mount", "-t", "erofs", "-o", f"ro,loop,device={loop_dev}", boot_path, MNT], check=True)
    t4 = time.monotonic()

  testbed = os.path.join(MNT, "layer0", "fs", "testbed")
  files_read = 0
  bytes_read = 0
  for dp, dns, fns in os.walk(testbed):
    dns[:] = sorted(d for d in dns if not d.startswith("."))
    for fn in sorted(fns):
      if fn.endswith(".py"):
        with open(os.path.join(dp, fn), "rb") as fp:
          bytes_read += len(fp.read())
        files_read += 1
        if files_read >= 1000:
          break
    if files_read >= 1000:
      break
  t5 = time.monotonic()

  w0 = time.monotonic()
  w_files = 0
  for dp, dns, fns in os.walk(testbed):
    dns[:] = sorted(d for d in dns if not d.startswith("."))
    for fn in sorted(fns):
      if fn.endswith(".py"):
        with open(os.path.join(dp, fn), "rb") as fp:
          _ = fp.read()
        w_files += 1
        if w_files >= 1000:
          break
    if w_files >= 1000:
      break
  w1 = time.monotonic()

  t_det0 = time.monotonic()
  subprocess.run(["umount", MNT], check=True)
  subprocess.run(["losetup", "-d", loop_dev], check=True)
  os.remove(boot_path)
  subprocess.run(["vdpa", "dev", "del", cid], check=True)
  subprocess.run(["/usr/local/bin/pool-ctl", "--vduse", "STOP", f"device_id={cid}"], check=True, capture_output=True)
  subprocess.run(["/usr/local/bin/pool-ctl", "--vduse", "DESTROY", f"device_id={cid}"], check=True, capture_output=True)
  t_det1 = time.monotonic()

  rec = {
      "trial": it,
      "num_zones": NUM_ZONES,
      "clone_ms": round((t1 - t0) * 1000, 2),
      "start_ms": round((t2 - t1) * 1000, 2),
      "vdpa_ms": round((t3 - t2) * 1000, 2),
      "stage_http_ms": round(stage_http_ms, 2),
      "stage_wait_after_vdpa_ms": round((t_pre - t3) * 1000, 2),
      "mount_ms": round((t4 - t_pre) * 1000, 2),
      "total_attach_ms": round((t4 - t0) * 1000, 2),
      "cold_read_1000_py_ms": round((t5 - t4) * 1000, 2),
      "warm_read_1000_py_ms": round((w1 - w0) * 1000, 2),
      "files_read": files_read,
      "bytes_read": bytes_read,
      "teardown_ms": round((t_det1 - t_det0) * 1000, 2),
  }
  print(json.dumps(rec), flush=True)
  trials.append(rec)

with open("/tmp/two-node-oci-erofs-share.json", "w") as out:
  json.dump({"gold_id": GOLD_ID, "num_zones": NUM_ZONES, "meta_bytes": META_BYTES, "node1_simultaneous_ro": True, "trials": trials}, out, indent=2)
os.chmod("/tmp/two-node-oci-erofs-share.json", 0o644)
EOF

gcloud compute scp "${RESULTS_ROOT}/node2_erofs_trials.py" "${NODE2}:/tmp/node2_erofs_trials.py" \
  --project="${PROJECT_ID}" --zone="${CLUSTER_LOCATION}" --quiet
gcloud compute ssh "${NODE2}" --project="${PROJECT_ID}" --zone="${CLUSTER_LOCATION}" --quiet \
  --command="sudo python3 /tmp/node2_erofs_trials.py" | tee "${RESULTS_ROOT}/two-node-oci-erofs-share.log"

gcloud compute scp "${NODE2}:/tmp/two-node-oci-erofs-share.json" "${RESULTS_ROOT}/two-node-oci-erofs-share.json" \
  --project="${PROJECT_ID}" --zone="${CLUSTER_LOCATION}" --quiet

gcloud compute ssh "${NODE1}" --project="${PROJECT_ID}" --zone="${CLUSTER_LOCATION}" --quiet --command="
  sudo umount /mnt/xnode-erofs-n1 2>/dev/null || true
  sudo vdpa dev del vduse-en1-583ef721 2>/dev/null || true
  sudo /usr/local/bin/pool-ctl --vduse STOP device_id=vduse-en1-583ef721 >/dev/null 2>&1 || true
  sudo /usr/local/bin/pool-ctl --vduse DESTROY device_id=vduse-en1-583ef721 >/dev/null 2>&1 || true
"
