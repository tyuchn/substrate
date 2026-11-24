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

"""Patch resolved nydus-dragonfly-seedhttp.yaml so control-plane + HTTP blob seed run on bench-role=driver and worker plugin runs on ate.dev/glovebox=true."""

import copy
import sys
import yaml

DRIVER_TOL = {"key": "bench-role", "operator": "Equal", "value": "driver", "effect": "NoSchedule"}
GLOVEBOX_TOL = {"key": "ate.dev/glovebox", "operator": "Equal", "value": "true", "effect": "NoSchedule"}


def set_env(container: dict, name: str, value: str) -> None:
  env = container.setdefault("env", [])
  for e in env:
    if e.get("name") == name:
      e["value"] = value
      return
  env.append({"name": name, "value": value})


def patch_one(doc: dict) -> dict | None:
  doc = copy.deepcopy(doc)
  kind = doc.get("kind")
  if kind not in ("Deployment", "DaemonSet"):
    return doc
  name = doc["metadata"]["name"]
  if name == "image-plugin-nydus-dragonfly-seed":
    # nydus-blob-seed nginx serves all preloaded blobs + manifests directly from /var/lib/ate/nydus-seed-blobs.
    return None

  spec = doc["spec"]["template"]["spec"]
  tols = list(spec.get("tolerations", []))

  if kind == "Deployment" or name == "nydus-blob-seed":
    spec["nodeSelector"] = {"bench-role": "driver"}
    tols.append(copy.deepcopy(DRIVER_TOL))
  elif name == "image-plugin-nydus-dragonfly":
    spec["nodeSelector"] = {"ate.dev/glovebox": "true"}
    tols.append(copy.deepcopy(GLOVEBOX_TOL))
  spec["tolerations"] = tols

  for c in spec.get("containers", []):
    if c["name"] == "dfdaemon":
      set_env(c, "DRAGONFLY_PREFETCH", "false")
    elif c["name"] == "nydus-snapshotter":
      set_env(c, "NYDUS_PREFETCH", "false")
  return doc


def main() -> None:
  docs = [copy.deepcopy(d) for d in yaml.safe_load_all(sys.stdin) if d]
  out_docs = []
  for doc in docs:
    if doc.get("kind") == "List":
      items = [p for it in doc.get("items", []) if (p := patch_one(it)) is not None]
      doc["items"] = items
      out_docs.append(doc)
    else:
      p = patch_one(doc)
      if p is not None:
        out_docs.append(p)
  yaml.safe_dump_all(out_docs, sys.stdout, sort_keys=False)


if __name__ == "__main__":
  main()
