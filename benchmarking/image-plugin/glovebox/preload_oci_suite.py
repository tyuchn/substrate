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

"""Mirror 16 SWE-bench OCI images to us-central1 AR, preload Golden Glovebox ZBDs, and build Nydus RAFS v6 images."""

from concurrent.futures import ThreadPoolExecutor, as_completed
import glob
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.request

PROJECT_ID = os.environ["PROJECT_ID"]
RAPID_BUCKET = os.environ.get("RAPID_BUCKET", f"ate-bench-rapid-{PROJECT_ID}")
AR_HOST = os.environ.get("AR_HOST", "us-central1-docker.pkg.dev")
AR_BASE = f"{AR_HOST}/{PROJECT_ID}/ate-images"
OCI_REPO = f"{AR_BASE}/swebench"
NYDUS_REPO = f"{AR_BASE}/swebench-nydus"
WORK_ROOT = "/mnt/work-tmpfs"
OUT_JSONL = "/tmp/images-16-bench.jsonl"
SHARED_JSONL = "/tmp/images-1-shared-rl.jsonl"
SUMMARY_JSON = "/tmp/preload-oci-summary.json"

# 16 stratified real SWE-bench images spanning 1.02 GB -> 3.15 GB compressed.
SELECTED_IMAGES = [
    ("psf__requests-2931", "sweb.eval.x86_64.psf_1776_requests-2931", 1020054732),
    ("pytest-dev__pytest-5262", "sweb.eval.x86_64.pytest-dev_1776_pytest-5262", 1031090646),
    ("sympy__sympy-13091", "sweb.eval.x86_64.sympy_1776_sympy-13091", 1063847785),
    ("pylint-dev__pylint-6903", "sweb.eval.x86_64.pylint-dev_1776_pylint-6903", 1081650296),
    ("sphinx-doc__sphinx-9591", "sweb.eval.x86_64.sphinx-doc_1776_sphinx-9591", 1101247284),
    ("sympy__sympy-23950", "sweb.eval.x86_64.sympy_1776_sympy-23950", 1105822129),
    ("django__django-11999", "sweb.eval.x86_64.django_1776_django-11999", 1154090521),
    ("django__django-13810", "sweb.eval.x86_64.django_1776_django-13810", 1158992263),
    ("django__django-16527", "sweb.eval.x86_64.django_1776_django-16527", 1241553818),
    ("scikit-learn__scikit-learn-14141", "sweb.eval.x86_64.scikit-learn_1776_scikit-learn-14141", 1495369648),
    ("scikit-learn__scikit-learn-14894", "sweb.eval.x86_64.scikit-learn_1776_scikit-learn-14894", 1497998369),
    ("pydata__xarray-4629", "sweb.eval.x86_64.pydata_1776_xarray-4629", 2052545000),
    ("matplotlib__matplotlib-20676", "sweb.eval.x86_64.matplotlib_1776_matplotlib-20676", 2258500000),
    ("matplotlib__matplotlib-24570", "sweb.eval.x86_64.matplotlib_1776_matplotlib-24570", 3142000000),
    ("matplotlib__matplotlib-24970", "sweb.eval.x86_64.matplotlib_1776_matplotlib-24970", 3144000000),
    ("matplotlib__matplotlib-25311", "sweb.eval.x86_64.matplotlib_1776_matplotlib-25311", 3148000000),
]

SYSTEM_PREFETCH = [
    "/etc/ld.so.cache",
    "/usr/lib/locale/C.utf8/LC_CTYPE",
    "/usr/lib/x86_64-linux-gnu/gconv/gconv-modules.cache",
    "/usr/lib/x86_64-linux-gnu/ld-linux-x86-64.so.2",
    "/usr/lib/x86_64-linux-gnu/libc.so.6",
    "/usr/lib/x86_64-linux-gnu/libdl.so.2",
    "/usr/lib/x86_64-linux-gnu/libm.so.6",
    "/usr/lib/x86_64-linux-gnu/libpthread.so.0",
    "/usr/lib/x86_64-linux-gnu/libutil.so.1",
]
ENV_LIBS = ("libbz2.so", "libcrypto.so", "libssl.so", "liblzma.so", "libz.so")


def run(cmd: list[str], env: dict | None = None, check: bool = True, capture: bool = False) -> subprocess.CompletedProcess:
  return subprocess.run(
      cmd,
      env=env,
      check=check,
      text=True,
      capture_output=capture,
  )


def refresh_docker_config(docker_dir: str) -> dict:
  os.makedirs(docker_dir, exist_ok=True)
  req = urllib.request.Request(
      "http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token",
      headers={"Metadata-Flavor": "Google"},
  )
  with urllib.request.urlopen(req, timeout=10) as resp:
    tok = json.loads(resp.read().decode())["access_token"]
  import base64
  auth_b64 = base64.b64encode(f"oauth2accesstoken:{tok}".encode()).decode()
  cfg = {"auths": {AR_HOST: {"auth": auth_b64}}}
  with open(os.path.join(docker_dir, "config.json"), "w", encoding="utf-8") as f:
    json.dump(cfg, f)
  env = os.environ.copy()
  env["DOCKER_CONFIG"] = docker_dir
  return env


def build_prefetch_list(rootfs_dir: str, reads: int = 1000) -> str:
  """Generate Nydus prefetch file list from unpacked rootfs matching guest.py access pattern."""
  picked: list[str] = []
  seen: set[str] = set()

  def add_real(rel_container_path: str) -> None:
    host_p = os.path.join(rootfs_dir, rel_container_path.lstrip("/"))
    if not os.path.lexists(host_p):
      return
    # Resolve symlinks inside rootfs_dir safely
    cur = rel_container_path
    for _ in range(8):
      hp = os.path.join(rootfs_dir, cur.lstrip("/"))
      if os.path.islink(hp):
        target = os.readlink(hp)
        if target.startswith("/"):
          cur = os.path.normpath(target)
        else:
          cur = os.path.normpath(os.path.join(os.path.dirname(cur), target))
      else:
        break
    hp_final = os.path.join(rootfs_dir, cur.lstrip("/"))
    if os.path.isfile(hp_final) and cur not in seen:
      seen.add(cur)
      picked.append(cur)

  for sp in SYSTEM_PREFETCH:
    add_real(sp)

  env_root = "/opt/miniconda3/envs/testbed"
  add_real(f"{env_root}/bin/python")
  lib_dir = os.path.join(rootfs_dir, env_root.lstrip("/"), "lib")
  if os.path.isdir(lib_dir):
    for entry in sorted(os.listdir(lib_dir)):
      if entry.startswith(ENV_LIBS) or entry.startswith("libpython"):
        add_real(f"{env_root}/lib/{entry}")
      if entry.startswith("python3.") and os.path.isdir(os.path.join(lib_dir, entry)):
        py_sub = os.path.join(lib_dir, entry)
        for dp, dns, fns in os.walk(py_sub):
          rel_dp = "/" + os.path.relpath(dp, rootfs_dir)
          if "site-packages" in rel_dp or "test" in rel_dp:
            dns[:] = []
            continue
          dns.sort()
          for fn in sorted(fns):
            if fn.endswith((".py", ".so")):
              add_real(f"{rel_dp}/{fn}")

  testbed = os.path.join(rootfs_dir, "testbed")
  found_py = 0
  if os.path.isdir(testbed):
    for dp, dns, fns in os.walk(testbed):
      dns[:] = sorted(d for d in dns if not d.startswith("."))
      for fn in sorted(fns):
        if fn.endswith(".py"):
          rel_f = "/" + os.path.relpath(os.path.join(dp, fn), rootfs_dir)
          add_real(rel_f)
          found_py += 1
          if found_py >= reads:
            break
      if found_py >= reads:
        break

  return "\n".join(picked) + "\n"


def unpack_oci_layers(oci_repo_path: str, layers: list[dict], fs_dir: str, env: dict) -> None:
  """Unpack OCI layers in bottom-up manifest order, applying OCI whiteouts and deferred hardlinks."""
  import tarfile
  for l_idx, layer in enumerate(layers):
    ldig = layer["digest"]
    blob_ref = f"{oci_repo_path}@{ldig}"
    proc = subprocess.Popen(
        ["/usr/local/bin/crane", "blob", blob_ref],
        stdout=subprocess.PIPE,
        env=env,
    )
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


def process_one(slot_idx: int, instance_id: str, repo_name: str, approx_bytes: int) -> dict:
  t_start = time.monotonic()
  slot_dir = os.path.join(WORK_ROOT, f"slot-{slot_idx:02d}")
  docker_dir = os.path.join(slot_dir, "docker")
  shutil.rmtree(slot_dir, ignore_errors=True)
  os.makedirs(slot_dir, exist_ok=True)
  env = refresh_docker_config(docker_dir)

  src_ref = f"docker.io/swebench/{repo_name}:latest"
  dst_oci_tag = f"{OCI_REPO}/{repo_name}:bench"
  t0 = time.monotonic()
  dig_probe = run(["/usr/local/bin/crane", "digest", dst_oci_tag], env=env, check=False, capture=True)
  if dig_probe.returncode == 0 and dig_probe.stdout.strip().startswith("sha256:"):
    oci_digest = dig_probe.stdout.strip()
    print(f"[{slot_idx:02d}] Reusing mirrored {dst_oci_tag} ({oci_digest[:19]})", flush=True)
  else:
    print(f"[{slot_idx:02d}] Mirroring {src_ref} -> {dst_oci_tag}", flush=True)
    run(["/usr/local/bin/crane", "copy", "--platform", "linux/amd64", src_ref, dst_oci_tag], env=env)
    oci_digest = run(["/usr/local/bin/crane", "digest", dst_oci_tag], env=env, capture=True).stdout.strip()
  oci_ref = f"{OCI_REPO}/{repo_name}@{oci_digest}"
  mirror_s = round(time.monotonic() - t0, 2)

  # Compute compressed size from manifest
  mf_raw = run(["/usr/local/bin/crane", "manifest", oci_ref], env=env, capture=True).stdout
  mf = json.loads(mf_raw)
  comp_bytes = sum(int(l.get("size", 0)) for l in mf.get("layers", []))
  num_layers = len(mf.get("layers", []))

  cfg_raw = run(["/usr/local/bin/crane", "config", oci_ref], env=env, capture=True).stdout
  cfg_obj = json.loads(cfg_raw).get("config", {})

  # Preload Golden Glovebox Zoned Btrfs ZBD (prefix with vduse- so device_id == vdpa device name)
  hex14 = oci_digest.removeprefix("sha256:")[:14]
  old_id = f"oci-g-{hex14}"
  run(["/usr/local/bin/pool-ctl", "--vduse", "STOP", f"device_id={old_id}"], check=False)
  run(["/usr/local/bin/pool-ctl", "--vduse", "DESTROY", f"device_id={old_id}"], check=False)

  gold_id = f"vduse-og-{hex14}"
  mnt_dir = f"/mnt/preload-zbd-{slot_idx:02d}"
  os.makedirs(mnt_dir, exist_ok=True)
  nydus_target = f"{NYDUS_REPO}/{repo_name}:pf"

  # Fast-path reuse if both Golden ZBD and Nydus RAFS v6 image already completed.
  nydus_probe = run(["/usr/local/bin/crane", "digest", nydus_target], env=env, check=False, capture=True)
  if nydus_probe.returncode == 0 and nydus_probe.stdout.strip().startswith("sha256:"):
    nydus_digest = nydus_probe.stdout.strip()
    nydus_ref = f"{NYDUS_REPO}/{repo_name}@{nydus_digest}"
    probe_clone = f"vduse-op-{slot_idx:02d}-{hex14[:8]}"
    run(["umount", "-l", mnt_dir], check=False)
    run(["vdpa", "dev", "del", probe_clone], check=False)
    run(["/usr/local/bin/pool-ctl", "--vduse", "STOP", f"device_id={probe_clone}"], check=False)
    run(["/usr/local/bin/pool-ctl", "--vduse", "DESTROY", f"device_id={probe_clone}"], check=False)
    clone_ok = run([
        "/usr/local/bin/pool-ctl", "--vduse", "--timeout_ms=60000",
        "CLONE", f"source_device_id={gold_id}", f"target_device_id={probe_clone}", "clone_mode=cow",
    ], check=False, capture=True)
    if clone_ok.returncode == 0:
      run([
          "/usr/local/bin/pool-ctl", "--vduse", "--timeout_ms=60000",
          "START", f"device_index={200 + slot_idx}", f"device_id={probe_clone}", f"parent_device_id={gold_id}",
          "zone_size_mb=256", "num_zones=128",
      ], capture=True)
      run(["vdpa", "dev", "add", "name", probe_clone, "mgmtdev", "vduse"])
      pblk = ""
      for _ in range(400):
        m = glob.glob(f"/sys/bus/vdpa/devices/{probe_clone}/virtio*/block/*")
        if m and os.path.exists("/dev/" + os.path.basename(m[0])):
          pblk = "/dev/" + os.path.basename(m[0])
          break
        time.sleep(0.01)
      run(["mount", "-t", "btrfs", "-o", "ro,noatime,nodiratime,nospace_cache,nodiscard,nodatasum", pblk, mnt_dir])
      df_out = run(["df", "-B1", "--output=used", mnt_dir], capture=True).stdout.splitlines()[-1].strip()
      unpacked_bytes = int(df_out)
      assert os.path.exists(os.path.join(mnt_dir, "oci-meta.json")), f"oci-meta.json missing in {gold_id}"
      run(["umount", mnt_dir])
      run(["vdpa", "dev", "del", probe_clone])
      run(["/usr/local/bin/pool-ctl", "--vduse", "STOP", f"device_id={probe_clone}"])
      run(["/usr/local/bin/pool-ctl", "--vduse", "DESTROY", f"device_id={probe_clone}"])
      shutil.rmtree(slot_dir, ignore_errors=True)
      print(f"[{slot_idx:02d}] VERIFIED cached {instance_id}: comp={comp_bytes/1e9:.2f}GB unpacked={unpacked_bytes/1e9:.2f}GB gold={gold_id}", flush=True)
      return {
          "idx": slot_idx,
          "instance_id": instance_id,
          "oci": oci_ref,
          "nydus": nydus_ref,
          "golden_device_id": gold_id,
          "compressed_bytes": comp_bytes,
          "unpacked_bytes": unpacked_bytes,
          "layers": num_layers,
          "mirror_s": mirror_s,
          "zbd_preload_s": 0.0,
          "nydus_convert_s": 0.0,
      }

  # Clean any prior leftover device/mount
  run(["umount", "-l", mnt_dir], check=False)
  run(["vdpa", "dev", "del", gold_id], check=False)
  run(["/usr/local/bin/pool-ctl", "--vduse", "STOP", f"device_id={gold_id}"], check=False)
  run(["/usr/local/bin/pool-ctl", "--vduse", "DESTROY", f"device_id={gold_id}"], check=False)

  t_zbd0 = time.monotonic()
  start_cp = run([
      "/usr/local/bin/pool-ctl", "--vduse", "--timeout_ms=120000",
      "START", f"device_index={100 + slot_idx}", f"device_id={gold_id}",
      "zone_size_mb=256", "num_zones=128",
  ], capture=True)
  vdpa_name = gold_id
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

  run(["mkfs.btrfs", "-f", "-O", "zoned", "-R", "^free-space-tree", "-m", "single", "-d", "single", blk])
  run(["mount", "-t", "btrfs", "-o", "noatime,nodiratime,nospace_cache,nodiscard,nodatasum", blk, mnt_dir])

  layer_dir = os.path.join(mnt_dir, "layer0")
  fs_dir = os.path.join(layer_dir, "fs")
  os.makedirs(fs_dir, exist_ok=True)

  print(f"[{slot_idx:02d}] Exporting {instance_id} into Golden ZBD {gold_id} ({blk})", flush=True)
  exp_cmd = f"/usr/local/bin/crane export '{oci_ref}' - | tar --numeric-owner -xpf - -C '{fs_dir}'"
  exp_res = run(["bash", "-o", "pipefail", "-c", exp_cmd], env=env, check=False)
  if exp_res.returncode != 0:
    print(f"[{slot_idx:02d}] crane export hit forward hardlinks on {instance_id}; re-extracting layer-by-layer", flush=True)
    shutil.rmtree(fs_dir, ignore_errors=True)
    os.makedirs(fs_dir, exist_ok=True)
    unpack_oci_layers(f"{OCI_REPO}/{repo_name}", mf.get("layers", []), fs_dir, env)

  with open(os.path.join(layer_dir, "whiteouts.json"), "w", encoding="utf-8") as f:
    json.dump({"version": 1}, f)
  with open(os.path.join(layer_dir, "finalized"), "w", encoding="utf-8") as f:
    f.write("")
  with open(os.path.join(mnt_dir, "oci-meta.json"), "w", encoding="utf-8") as f:
    json.dump({
        "instance_id": instance_id,
        "image_digest": oci_digest,
        "golden_device_id": gold_id,
        "compressed_bytes": comp_bytes,
        "config": cfg_obj,
    }, f)

  # Build prefetch list directly from unpacked fs_dir BEFORE unmounting!
  prefetch_txt = build_prefetch_list(fs_dir, reads=1000)
  prefetch_path = os.path.join(slot_dir, "prefetch.txt")
  with open(prefetch_path, "w", encoding="utf-8") as f:
    f.write(prefetch_txt)

  df_out = run(["df", "-B1", "--output=used", mnt_dir], capture=True).stdout.splitlines()[-1].strip()
  unpacked_bytes = int(df_out)

  run(["sync", "-f", mnt_dir])
  run(["umount", mnt_dir])
  run(["vdpa", "dev", "del", vdpa_name])
  run(["/usr/local/bin/pool-ctl", "--vduse", "--timeout_ms=120000", "STOP", f"device_id={gold_id}"])
  zbd_preload_s = round(time.monotonic() - t_zbd0, 2)

  # Refresh token before Nydus conversion
  env = refresh_docker_config(docker_dir)
  nydus_target = f"{NYDUS_REPO}/{repo_name}:pf"
  nydus_work = os.path.join(slot_dir, "nydus-work")
  os.makedirs(nydus_work, exist_ok=True)
  print(f"[{slot_idx:02d}] Converting {instance_id} -> Nydus RAFS v6 ({nydus_target})", flush=True)
  t_nyd0 = time.monotonic()
  with open(prefetch_path, "r", encoding="utf-8") as pf_in, open(os.path.join(slot_dir, "nydusify.log"), "w", encoding="utf-8") as nlog:
    subprocess.run(
        [
            "/usr/local/bin/nydusify", "convert",
            "--source", oci_ref,
            "--target", nydus_target,
            "--fs-version", "6",
            "--compressor", "zstd",
            "--nydus-image", "/usr/local/bin/nydus-image",
            "--work-dir", nydus_work,
            "--prefetch-patterns",
        ],
        stdin=pf_in,
        stdout=nlog,
        stderr=subprocess.STDOUT,
        env=env,
        check=True,
    )
  nydus_digest = run(["/usr/local/bin/crane", "digest", nydus_target], env=env, capture=True).stdout.strip()
  nydus_ref = f"{NYDUS_REPO}/{repo_name}@{nydus_digest}"
  nydus_s = round(time.monotonic() - t_nyd0, 2)

  shutil.rmtree(slot_dir, ignore_errors=True)
  total_s = round(time.monotonic() - t_start, 2)
  print(
      f"[{slot_idx:02d}] DONE {instance_id}: comp={comp_bytes/1e9:.2f}GB "
      f"unpacked={unpacked_bytes/1e9:.2f}GB gold={gold_id} "
      f"mirror={mirror_s}s zbd={zbd_preload_s}s nydus={nydus_s}s total={total_s}s",
      flush=True,
  )
  return {
      "idx": slot_idx,
      "instance_id": instance_id,
      "oci": oci_ref,
      "nydus": nydus_ref,
      "golden_device_id": gold_id,
      "compressed_bytes": comp_bytes,
      "unpacked_bytes": unpacked_bytes,
      "layers": num_layers,
      "mirror_s": mirror_s,
      "zbd_preload_s": zbd_preload_s,
      "nydus_convert_s": nydus_s,
  }


def main() -> None:
  t_all = time.monotonic()
  results: list[dict] = []
  with ThreadPoolExecutor(max_workers=4) as ex:
    futs = {
        ex.submit(process_one, idx, inst, rname, sz): (idx, inst)
        for idx, (inst, rname, sz) in enumerate(SELECTED_IMAGES)
    }
    for fut in as_completed(futs):
      idx, inst = futs[fut]
      res = fut.result()
      results.append(res)

  results.sort(key=lambda x: x["idx"])
  with open(OUT_JSONL, "w", encoding="utf-8") as f:
    for r in results:
      f.write(json.dumps({
          "instance_id": r["instance_id"],
          "oci": r["oci"],
          "nydus": r["nydus"],
          "golden_device_id": r["golden_device_id"],
          "compressed_bytes": r["compressed_bytes"],
          "unpacked_bytes": r["unpacked_bytes"],
          "layers": r["layers"],
      }) + "\n")

  shared_item = next((r for r in results if r["instance_id"] == "pydata__xarray-4629"), results[0])
  with open(SHARED_JSONL, "w", encoding="utf-8") as f:
    f.write(json.dumps({
        "instance_id": shared_item["instance_id"],
        "oci": shared_item["oci"],
        "nydus": shared_item["nydus"],
        "golden_device_id": shared_item["golden_device_id"],
        "compressed_bytes": shared_item["compressed_bytes"],
        "unpacked_bytes": shared_item["unpacked_bytes"],
        "layers": shared_item["layers"],
    }) + "\n")

  summary = {
      "total_wall_s": round(time.monotonic() - t_all, 2),
      "count": len(results),
      "images": results,
  }
  with open(SUMMARY_JSON, "w", encoding="utf-8") as f:
    json.dump(summary, f, indent=2)
  print(f"=== ALL 16 IMAGES READY IN {summary['total_wall_s']}s ===", flush=True)


if __name__ == "__main__":
  main()
