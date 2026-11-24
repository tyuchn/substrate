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

"""Aggregate Scenario A and Scenario B turns.jsonl, host RAM/page-cache telemetry, Glovebox ZBD attach timings, and microVM restore phase timings."""

import glob
import json
import math
import os
import re

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("DATA_DIR", os.path.join(SCRIPT_DIR, "data"))
RESULTS_ROOT = os.environ.get("RESULTS_ROOT", os.path.join(SCRIPT_DIR, "results"))


def pct(vals: list[float], q: float) -> float:
  if not vals:
    return 0.0
  s = sorted(vals)
  if len(s) == 1:
    return round(s[0], 4)
  pos = (len(s) - 1) * (q / 100.0)
  lo = int(math.floor(pos))
  hi = int(math.ceil(pos))
  if lo == hi:
    return round(s[lo], 4)
  return round(s[lo] + (s[hi] - s[lo]) * (pos - lo), 4)


def stats(vals: list[float]) -> dict:
  if not vals:
    return {"count": 0, "p50": 0.0, "p95": 0.0, "max": 0.0, "mean": 0.0}
  return {
      "count": len(vals),
      "p50": pct(vals, 50),
      "p95": pct(vals, 95),
      "max": round(max(vals), 4),
      "mean": round(sum(vals) / len(vals), 4),
  }


def parse_memory(path: str) -> dict:
  if not os.path.exists(path):
    return {}
  rows = []
  with open(path, "r", encoding="utf-8") as f:
    for line in f:
      line = line.strip()
      if line.startswith("{") and line.endswith("}"):
        try:
          rows.append(json.loads(line))
        except json.JSONDecodeError:
          pass
  if not rows:
    return {}
  base = rows[0]
  samples = rows[1:] if len(rows) > 1 else rows
  base_avail = base.get("mem_available_kb", 0) / 1024.0
  base_cached = base.get("cached_kb", 0) / 1024.0
  base_rss = base.get("zbd_daemon_rss_kb", 0) / 1024.0

  min_avail = min(r.get("mem_available_kb", base.get("mem_available_kb", 0)) for r in samples) / 1024.0
  max_cached = max(r.get("cached_kb", base.get("cached_kb", 0)) for r in samples) / 1024.0
  max_rss = max(r.get("zbd_daemon_rss_kb", 0) for r in samples) / 1024.0
  max_gb_mnt = max(r.get("glovebox_mounts", 0) for r in samples)
  max_er_mnt = max(r.get("erofs_mounts", 0) for r in samples)

  return {
      "baseline_mem_avail_mib": round(base_avail, 1),
      "min_mem_avail_mib": round(min_avail, 1),
      "peak_mem_used_delta_mib": round(max(0.0, base_avail - min_avail), 1),
      "baseline_cached_mib": round(base_cached, 1),
      "peak_cached_mib": round(max_cached, 1),
      "peak_cached_delta_mib": round(max(0.0, max_cached - base_cached), 1),
      "baseline_zbd_rss_mib": round(base_rss, 1),
      "peak_zbd_rss_mib": round(max_rss, 1),
      "peak_zbd_rss_delta_mib": round(max(0.0, max_rss - base_rss), 1),
      "peak_glovebox_mounts": max_gb_mnt,
      "peak_erofs_mounts": max_er_mnt,
  }


def parse_glovebox_timings(path: str) -> dict:
  if not os.path.exists(path):
    return {}
  pat = re.compile(
      r"(?:num_zones=([0-9]+)\s+)?(?:meta_warm_ms=([0-9.]+)\s+)?"
      r"clone_ms=([0-9.]+)\s+start_ms=([0-9.]+)\s+vdpa_ms=([0-9.]+)\s+mount_ms=([0-9.]+)\s+total_ms=([0-9.]+)"
  )
  zones, warms, clones, starts, vdpas, mounts, totals = [], [], [], [], [], [], []
  with open(path, "r", encoding="utf-8") as f:
    for line in f:
      m = pat.search(line)
      if m:
        if m.group(1):
          zones.append(float(m.group(1)))
        if m.group(2):
          warms.append(float(m.group(2)))
        clones.append(float(m.group(3)))
        starts.append(float(m.group(4)))
        vdpas.append(float(m.group(5)))
        mounts.append(float(m.group(6)))
        totals.append(float(m.group(7)))
  if not totals:
    return {}
  res = {
      "attaches": len(totals),
      "clone_ms": stats(clones),
      "start_ms": stats(starts),
      "vdpa_ms": stats(vdpas),
      "mount_ms": stats(mounts),
      "total_ms": stats(totals),
  }
  if zones:
    res["num_zones"] = stats(zones)
  if warms:
    res["meta_warm_ms"] = stats(warms)
  return res


def parse_restore_phases(atelet_path: str, workers_path: str) -> dict:
  out = {}
  if os.path.exists(atelet_path):
    unpacks, restores = [], []
    with open(atelet_path, "r", encoding="utf-8") as f:
      for line in f:
        m_u = re.search(r"oci_unpack[=\": ]+([0-9.]+)(ms|s)", line)
        if m_u:
          val = float(m_u.group(1)) * (1.0 if m_u.group(2) == "s" else 0.001)
          unpacks.append(val)
        m_r = re.search(r"worker_restore[=\": ]+([0-9.]+)(ms|s)", line)
        if m_r:
          val = float(m_r.group(1)) * (1.0 if m_r.group(2) == "s" else 0.001)
          restores.append(val)
    if unpacks:
      out["atelet_oci_unpack_s"] = stats(unpacks)
    if restores:
      out["atelet_worker_restore_s"] = stats(restores)

  if os.path.exists(workers_path):
    vm_restores = []
    with open(workers_path, "r", encoding="utf-8") as f:
      for line in f:
        m_v = re.search(r"vm_restore[=\": ]+([0-9.]+)(ms|s)", line)
        if m_v:
          val = float(m_v.group(1)) * (1.0 if m_v.group(2) == "s" else 0.001)
          vm_restores.append(val)
    if vm_restores:
      out["worker_vm_restore_s"] = stats(vm_restores)
  return out


def main() -> None:
  img_meta = {}
  for fn in ("images-16-bench-erofs.jsonl", "images-16-bench.jsonl"):
    img_jsonl = os.path.join(DATA_DIR, fn)
    if os.path.exists(img_jsonl):
      with open(img_jsonl, "r", encoding="utf-8") as f:
        for line in f:
          if line.strip():
            o = json.loads(line)
            img_meta[o["instance_id"]] = o
      break

  out_summary = {}
  for run_dir in sorted(glob.glob(os.path.join(RESULTS_ROOT, "scenario-*"))):
    if not os.path.isdir(run_dir):
      continue
    name = os.path.basename(run_dir)
    turns_path = os.path.join(run_dir, "turns.jsonl")
    if not os.path.exists(turns_path):
      continue
    rows = []
    with open(turns_path, "r", encoding="utf-8") as f:
      for line in f:
        if line.strip():
          rows.append(json.loads(line))

    by_turn = {}
    for t_idx in (0, 1):
      sub = [r for r in rows if r.get("turn") == t_idx]
      ok = [r for r in sub if not r.get("error")]
      errs = [r for r in sub if r.get("error")]
      by_turn[f"turn_{t_idx}"] = {
          "total": len(sub),
          "errors": len(errs),
          "ttfi_s": stats([r["ttfi_s"] for r in ok]),
          "resume_s": stats([r["resume_s"] for r in ok]),
          "queue_s": stats([r["queue_s"] for r in ok]),
          "read_s": stats([r["read_s"] for r in ok]),
          "write_s": stats([r["write_s"] for r in ok]),
          "work_s": stats([r["work_s"] for r in ok]),
          "task_s": stats([r["task_s"] for r in ok]),
          "suspend_s": stats([r["suspend_s"] for r in ok]),
          "cold_total_ready_plus_work_s": stats([r["ttfi_s"] + r["work_call_s"] for r in ok]),
      }
      if name.startswith("scenario-a-") and t_idx == 0:
        per_img = []
        for r in ok:
          im = img_meta.get(r["image"], {})
          per_img.append({
              "image": r["image"],
              "compressed_gb": round(im.get("compressed_bytes", 0) / 1e9, 2),
              "unpacked_gb": round(im.get("unpacked_bytes", 0) / 1e9, 2),
              "erofs_zones": im.get("erofs_num_zones", 0),
              "ttfi_s": round(r["ttfi_s"], 3),
              "resume_s": round(r["resume_s"], 3),
              "read_s": round(r["read_s"], 3),
              "work_s": round(r["work_s"], 3),
              "ttfi_plus_work_s": round(r["ttfi_s"] + r["work_call_s"], 3),
          })
        per_img.sort(key=lambda x: x["compressed_gb"])
        by_turn["turn_0"]["per_image"] = per_img

    out_summary[name] = {
        "turns": by_turn,
        "memory": parse_memory(os.path.join(run_dir, "node-memory.jsonl")),
        "glovebox_attach": parse_glovebox_timings(os.path.join(run_dir, "glovebox-plugin.log")),
        "restore_phases": parse_restore_phases(
            os.path.join(run_dir, "atelet.log"),
            os.path.join(run_dir, "workers-restore.log"),
        ),
    }

  out_file = os.path.join(RESULTS_ROOT, "oci-benchmark-summary.json")
  with open(out_file, "w", encoding="utf-8") as f:
    json.dump(out_summary, f, indent=2)
  print(f"Wrote {out_file} with {len(out_summary)} runs")


if __name__ == "__main__":
  main()
