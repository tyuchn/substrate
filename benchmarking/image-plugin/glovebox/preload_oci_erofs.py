#!/usr/bin/env python3
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

"""Preload SWE-bench OCI images onto right-sized Contiguous-Metadata EROFS Golden Glovebox ZBDs (vduse-oe-<hex14>)."""

import base64
from concurrent.futures import ThreadPoolExecutor, as_completed
import glob
import json
import os
import shutil
import struct
import subprocess
import tarfile
import time
import urllib.request

AR_HOST = os.environ.get("AR_HOST", "us-central1-docker.pkg.dev")
WORK_ROOT = os.environ.get("WORK_ROOT", "/mnt/work-tmpfs/erofs-preload")
IN_JSONL = os.environ.get("IN_JSONL", "/tmp/images-16-bench.jsonl")
OUT_JSONL = os.environ.get("OUT_JSONL", "/tmp/images-16-bench-erofs.jsonl")
SUMMARY_JSON = os.environ.get("SUMMARY_JSON", "/tmp/preload-oci-erofs-summary.json")

ZONE_BYTES = 256 * 1024 * 1024
ALIGN_4M = 4 * 1024 * 1024


def run(cmd: list[str], env: dict | None = None, check: bool = True, capture: bool = False) -> subprocess.CompletedProcess:
  return subprocess.run(cmd, env=env, check=check, text=True, capture_output=capture)


def refresh_docker_config(docker_dir: str) -> dict:
  os.makedirs(docker_dir, exist_ok=True)
  req = urllib.request.Request(
      "http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token",
      headers={"Metadata-Flavor": "Google"},
  )
  with urllib.request.urlopen(req, timeout=10) as resp:
    tok = json.loads(resp.read().decode())["access_token"]
  auth_b64 = base64.b64encode(f"oauth2accesstoken:{tok}".encode()).decode()
  with open(os.path.join(docker_dir, "config.json"), "w", encoding="utf-8") as f:
    json.dump({"auths": {AR_HOST: {"auth": auth_b64}}}, f)
  env = os.environ.copy()
  env["DOCKER_CONFIG"] = docker_dir
  return env


def unpack_oci_layers(oci_repo_path: str, layers: list[dict], fs_dir: str, env: dict) -> None:
  for layer in layers:
    ldig = layer["digest"]
    blob_ref = f"{oci_repo_path}@{ldig}"
    proc = subprocess.Popen(["/usr/local/bin/crane", "blob", blob_ref], stdout=subprocess.PIPE, env=env)
    deferred_links: list[tuple[tarfile.TarInfo, str]] = []
    assert proc.stdout is not None
    with tarfile.open(fileobj=proc.stdout, mode="r|*") as tf:
      for member in tf:
        rel = os.path.normpath(member.name).lstrip("/")
        if not rel or rel == "." or rel.startswith(".."):
          continue
        base = os.path.basename(rel)
        parent = os.path.join(fs_dir, os.path.dirname(rel))
        if base == ".wh..wh..opq":
          if os.path.isdir(parent):
            for entry in os.listdir(parent):
              p = os.path.join(parent, entry)
              if os.path.isdir(p) and not os.path.islink(p):
                shutil.rmtree(p, ignore_errors=True)
              else:
                try:
                  os.unlink(p)
                except FileNotFoundError:
                  pass
          continue
        if base.startswith(".wh."):
          victim = os.path.join(parent, base[4:])
          if os.path.isdir(victim) and not os.path.islink(victim):
            shutil.rmtree(victim, ignore_errors=True)
          else:
            try:
              os.unlink(victim)
            except FileNotFoundError:
              pass
          continue

        dst = os.path.join(fs_dir, rel)
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        if os.path.lexists(dst):
          if os.path.isdir(dst) and not os.path.islink(dst) and member.isdir():
            pass
          elif os.path.isdir(dst) and not os.path.islink(dst):
            shutil.rmtree(dst, ignore_errors=True)
          else:
            try:
              os.unlink(dst)
            except FileNotFoundError:
              pass

        if member.islnk():
          link_target = os.path.join(fs_dir, os.path.normpath(member.linkname).lstrip("/"))
          if os.path.exists(link_target):
            os.link(link_target, dst)
          else:
            deferred_links.append((member, dst))
          continue

        member.name = rel
        tf.extract(member, path=fs_dir, set_attrs=True, filter="fully_trusted")

    rc = proc.wait()
    if rc != 0:
      raise RuntimeError(f"crane blob {blob_ref} exited with {rc}")

    for member, dst in deferred_links:
      link_target = os.path.join(fs_dir, os.path.normpath(member.linkname).lstrip("/"))
      if os.path.exists(link_target):
        if os.path.lexists(dst):
          os.unlink(dst)
        os.link(link_target, dst)


def build_and_preload_one(slot_idx: int, item: dict) -> dict:
  t0 = time.monotonic()
  instance_id = item["instance_id"]
  oci_ref = item["oci"]
  oci_repo_path, oci_digest = oci_ref.split("@", 1)
  hex14 = oci_digest.removeprefix("sha256:")[:14]
  gold_erofs_id = f"vduse-oe-{hex14}"

  slot_dir = os.path.join(WORK_ROOT, f"slot-{slot_idx:02d}")
  shutil.rmtree(slot_dir, ignore_errors=True)
  os.makedirs(slot_dir, exist_ok=True)
  env = refresh_docker_config(os.path.join(slot_dir, "docker"))

  mf = json.loads(run(["/usr/local/bin/crane", "manifest", oci_ref], env=env, capture=True).stdout)
  cfg_obj = json.loads(run(["/usr/local/bin/crane", "config", oci_ref], env=env, capture=True).stdout).get("config", {})

  src_dir = os.path.join(slot_dir, "src")
  layer_dir = os.path.join(src_dir, "layer0")
  fs_dir = os.path.join(layer_dir, "fs")
  os.makedirs(fs_dir, exist_ok=True)

  print(f"[{slot_idx:02d}] Unpacking {instance_id} onto tmpfs...", flush=True)
  t_unp0 = time.monotonic()
  exp_cmd = f"/usr/local/bin/crane export '{oci_ref}' - | tar --numeric-owner -xpf - -C '{fs_dir}'"
  exp_res = run(["bash", "-o", "pipefail", "-c", exp_cmd], env=env, check=False)
  if exp_res.returncode != 0:
    print(f"[{slot_idx:02d}] crane export hit forward hardlinks on {instance_id}; extracting layer-by-layer", flush=True)
    shutil.rmtree(fs_dir, ignore_errors=True)
    os.makedirs(fs_dir, exist_ok=True)
    unpack_oci_layers(oci_repo_path, mf.get("layers", []), fs_dir, env)
  unpack_s = round(time.monotonic() - t_unp0, 2)

  with open(os.path.join(layer_dir, "whiteouts.json"), "w", encoding="utf-8") as f:
    json.dump({"version": 1}, f)
  with open(os.path.join(layer_dir, "finalized"), "w", encoding="utf-8") as f:
    f.write("")
  with open(os.path.join(src_dir, "oci-meta.json"), "w", encoding="utf-8") as f:
    json.dump({
        "instance_id": instance_id,
        "image_digest": oci_digest,
        "golden_device_id": gold_erofs_id,
        "compressed_bytes": item["compressed_bytes"],
        "config": cfg_obj,
    }, f)

  meta_p = os.path.join(slot_dir, "meta.erofs")
  blob_p = os.path.join(slot_dir, "data.blob")
  open(blob_p, "wb").close()

  t_mk0 = time.monotonic()
  run([
      "mkfs.erofs",
      "--chunksize=1048576",
      f"--blobdev={blob_p}",
      "-E", "nosbcrc",
      "-T", "0",
      "--quiet",
      meta_p,
      src_dir,
  ])
  mkfs_s = round(time.monotonic() - t_mk0, 2)
  shutil.rmtree(src_dir, ignore_errors=True)

  raw_meta_sz = os.path.getsize(meta_p)
  raw_blob_sz = os.path.getsize(blob_p)
  meta_aligned = (raw_meta_sz + ALIGN_4M - 1) // ALIGN_4M * ALIGN_4M
  blob_aligned = (raw_blob_sz + ALIGN_4M - 1) // ALIGN_4M * ALIGN_4M
  meta_blocks = meta_aligned // 4096
  blob_blocks = (raw_blob_sz + 4095) // 4096

  with open(meta_p, "r+b") as f:
    f.truncate(meta_aligned)
    f.seek(1024)
    sb = bytearray(f.read(128))
    magic, = struct.unpack_from("<I", sb, 0)
    if magic != 0xE0F5E1E2:
      raise RuntimeError(f"bad EROFS magic {hex(magic)} for {instance_id}")
    extra_devs, devt_slotoff = struct.unpack_from("<HH", sb, 86)
    if extra_devs != 1 or devt_slotoff == 0:
      raise RuntimeError(f"unexpected EROFS extra_devs={extra_devs} devt_slotoff={devt_slotoff}")
    struct.pack_into("<I", sb, 36, meta_blocks + blob_blocks)
    f.seek(1024)
    f.write(sb)

    slot_off = devt_slotoff * 128
    f.seek(slot_off)
    slot = bytearray(f.read(128))
    slot[:64] = b"\x00" * 64
    struct.pack_into("<II", slot, 64, blob_blocks, meta_blocks)
    f.seek(slot_off)
    f.write(slot)

  with open(blob_p, "r+b") as f:
    f.truncate(blob_aligned)

  total_erofs_bytes = meta_aligned + blob_aligned
  num_zones = max(2, (total_erofs_bytes + ZONE_BYTES - 1) // ZONE_BYTES)

  mnt_check = f"/mnt/erofs-verify-{slot_idx:02d}"
  run(["umount", "-l", mnt_check], check=False)
  run(["vdpa", "dev", "del", gold_erofs_id], check=False)
  run(["/usr/local/bin/pool-ctl", "--vduse", "STOP", f"device_id={gold_erofs_id}"], check=False)
  run(["/usr/local/bin/pool-ctl", "--vduse", "DESTROY", f"device_id={gold_erofs_id}"], check=False)

  t_zbd0 = time.monotonic()
  start_cp = run([
      "/usr/local/bin/pool-ctl", "--vduse", "--timeout_ms=120000",
      "START", f"device_index={150 + slot_idx}", f"device_id={gold_erofs_id}",
      "zone_size_mb=256", f"num_zones={num_zones}",
  ], capture=True)
  vdpa_name = gold_erofs_id
  for tok in start_cp.stdout.split():
    if tok.startswith("socket_path="):
      vdpa_name = tok.split("=", 1)[1].strip()

  run(["vdpa", "dev", "add", "name", vdpa_name, "mgmtdev", "vduse"])
  blk = ""
  for _ in range(400):
    m = glob.glob(f"/sys/bus/vdpa/devices/{vdpa_name}/virtio*/block/*")
    if m and os.path.exists("/dev/" + os.path.basename(m[0])):
      blk = "/dev/" + os.path.basename(m[0])
      break
    time.sleep(0.01)
  if not blk:
    raise RuntimeError(f"Block device did not appear for {vdpa_name}")

  stream_cmd = f"cat '{meta_p}' '{blob_p}' | dd of='{blk}' bs=4M iflag=fullblock oflag=direct conv=fsync status=none"
  run(["bash", "-o", "pipefail", "-c", stream_cmd])

  os.makedirs(mnt_check, exist_ok=True)
  run(["mount", "-t", "erofs", "-o", "ro", blk, mnt_check])
  assert os.path.exists(os.path.join(mnt_check, "oci-meta.json"))
  assert os.path.exists(os.path.join(mnt_check, "layer0", "finalized"))
  py_bin = os.path.join(mnt_check, "layer0", "fs", "opt", "miniconda3", "envs", "testbed", "bin", "python")
  if os.path.exists(py_bin):
    run([py_bin, "-V"], capture=True)
  run(["umount", mnt_check])
  os.rmdir(mnt_check)

  run(["vdpa", "dev", "del", vdpa_name])
  run(["/usr/local/bin/pool-ctl", "--vduse", "--timeout_ms=120000", "STOP", f"device_id={gold_erofs_id}"])
  zbd_write_s = round(time.monotonic() - t_zbd0, 2)

  shutil.rmtree(slot_dir, ignore_errors=True)
  wall_s = round(time.monotonic() - t0, 2)
  print(
      f"[{slot_idx:02d}] DONE {instance_id}: gold={gold_erofs_id} zones={num_zones} "
      f"erofs={total_erofs_bytes/1e9:.2f}GB meta={raw_meta_sz/1048576:.2f}MiB "
      f"unpack={unpack_s}s mkfs={mkfs_s}s zbd={zbd_write_s}s total={wall_s}s",
      flush=True,
  )
  out = dict(item)
  out["golden_erofs_device_id"] = gold_erofs_id
  out["erofs_num_zones"] = num_zones
  out["erofs_bytes"] = total_erofs_bytes
  out["erofs_meta_bytes"] = raw_meta_sz
  out["erofs_meta_aligned_bytes"] = meta_aligned
  out["erofs_mkfs_s"] = mkfs_s
  out["erofs_zbd_write_s"] = zbd_write_s
  return out


def main() -> None:
  t_all = time.monotonic()
  os.makedirs(WORK_ROOT, exist_ok=True)
  with open(IN_JSONL, "r", encoding="utf-8") as f:
    items = [json.loads(line) for line in f if line.strip()]

  results: list[tuple[int, dict]] = []
  with ThreadPoolExecutor(max_workers=3) as ex:
    futs = {ex.submit(build_and_preload_one, idx, it): idx for idx, it in enumerate(items)}
    for fut in as_completed(futs):
      idx = futs[fut]
      results.append((idx, fut.result()))

  results.sort(key=lambda x: x[0])
  ordered = [r for _, r in results]
  with open(OUT_JSONL, "w", encoding="utf-8") as f:
    for r in ordered:
      f.write(json.dumps(r) + "\n")

  summary = {
      "total_wall_s": round(time.monotonic() - t_all, 2),
      "count": len(ordered),
      "images": ordered,
  }
  with open(SUMMARY_JSON, "w", encoding="utf-8") as f:
    json.dump(summary, f, indent=2)
  print(f"=== ALL {len(ordered)} RIGHT-SIZED EROFS GOLDEN ZBDS READY IN {summary['total_wall_s']}s ===", flush=True)


if __name__ == "__main__":
  main()
