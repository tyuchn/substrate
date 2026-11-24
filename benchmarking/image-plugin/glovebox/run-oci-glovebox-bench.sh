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
ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
DATA_DIR="${DATA_DIR:-${SCRIPT_DIR}/data}"
RESULTS_ROOT="${RESULTS_ROOT:-${SCRIPT_DIR}/results}"
mkdir -p "${RESULTS_ROOT}"

if [[ -f "${ROOT}/.ate-dev-env.sh" ]]; then
  # shellcheck disable=SC1091
  source "${ROOT}/.ate-dev-env.sh"
fi

PROJECT_ID="${PROJECT_ID:?set PROJECT_ID}"
CLUSTER_LOCATION="${CLUSTER_LOCATION:?set CLUSTER_LOCATION}"
RAPID_BUCKET="${RAPID_BUCKET:-ate-bench-rapid-${PROJECT_ID}}"
SNAPSHOT_LOCATION="${SNAPSHOT_LOCATION:-gs://ate-bench-${PROJECT_ID}/image-plugin-bench/}"

cd "${ROOT}"

KO="$(cd "${ROOT}/hack/tools/ko" && go tool -n ko)"
UBUNTU_NODE="$(kubectl get nodes -l cloud.google.com/gke-nodepool=substrate-ubuntu-pool -o jsonpath='{.items[0].metadata.name}')"

cmd="${1:?usage: run-oci-glovebox-bench.sh setup|switch-plugin|bake|run-arm ...}"
shift

remove_all_plugins() {
  kubectl -n ate-system delete ds image-plugin-glovebox image-plugin-nydus-dragonfly --ignore-not-found --wait=true >/dev/null 2>&1 || true
  gcloud compute ssh "${UBUNTU_NODE}" --project="${PROJECT_ID}" --zone="${CLUSTER_LOCATION}" --quiet \
    --command="sudo rm -f /run/ate/image-plugin.sock /run/ate/drain-complete"
}

switch_plugin() {
  local arm="$1"
  echo "=== Switching ImagePlugin on ${UBUNTU_NODE} to arm: ${arm} ==="
  case "${arm}" in
    pull)
      remove_all_plugins
      ;;
    glovebox-shared-ro|glovebox-per-actor-cow|glovebox-shared-erofs)
      kubectl -n ate-system delete ds image-plugin-nydus-dragonfly --ignore-not-found --wait=true >/dev/null 2>&1 || true
      local mode="shared-ro"
      local fstype="btrfs"
      [[ "${arm}" == "glovebox-per-actor-cow" ]] && mode="per-actor-cow"
      [[ "${arm}" == "glovebox-shared-erofs" ]] && fstype="erofs"
      if [[ -f "${DATA_DIR}/images-16-bench-erofs.jsonl" ]]; then
        gcloud compute scp "${DATA_DIR}/images-16-bench-erofs.jsonl" "${UBUNTU_NODE}:/tmp/images-16-bench-erofs-stage.jsonl" \
          --project="${PROJECT_ID}" --zone="${CLUSTER_LOCATION}" --quiet
        gcloud compute ssh "${UBUNTU_NODE}" --project="${PROJECT_ID}" --zone="${CLUSTER_LOCATION}" --quiet \
          --command="sudo mkdir -p /var/lib/ate/image-plugin-cache/glovebox && sudo mv -f /tmp/images-16-bench-erofs-stage.jsonl /var/lib/ate/image-plugin-cache/glovebox/images-16-bench-erofs.jsonl"
      fi
      local out_yaml="${RESULTS_ROOT}/glovebox-${mode}-${fstype}.applied.yaml"
      "${KO}" resolve --base-import-paths -f image-plugin/manifests/glovebox-oci.yaml 2>/dev/null |
        sed -e "s|--mode=shared-ro|--mode=${mode}|g" \
            -e "s|--fs=btrfs|--fs=${fstype}|g" \
            -e "s|RAPID_BUCKET_PLACEHOLDER|${RAPID_BUCKET}|g" > "${out_yaml}"
      kubectl -n ate-system delete ds image-plugin-glovebox --ignore-not-found --wait=true >/dev/null 2>&1 || true
      gcloud compute ssh "${UBUNTU_NODE}" --project="${PROJECT_ID}" --zone="${CLUSTER_LOCATION}" --quiet \
        --command="sudo /usr/local/bin/reset-cold-node.sh >/dev/null"
      kubectl apply -f "${out_yaml}"
      kubectl -n ate-system rollout status ds/image-plugin-glovebox --timeout=5m
      ;;
    nydus)
      kubectl -n ate-system delete ds image-plugin-glovebox --ignore-not-found --wait=true >/dev/null 2>&1 || true
      local nydusd_img
      nydusd_img="$(tail -n 1 "${DATA_DIR}/nydusd-image.txt" | tr -d '[:space:]')"
      local nydus_yaml="${RESULTS_ROOT}/nydus-seedhttp.applied.yaml"
      "${KO}" resolve --base-import-paths -f image-plugin/manifests/nydus-dragonfly-seedhttp.yaml 2>/dev/null |
        python3 "${SCRIPT_DIR}/prepare-nydus-arm.py" |
        python3 "${ROOT}/benchmarking/image-plugin/nydusd-amp/use-nydusd.py" "${nydusd_img}" > "${nydus_yaml}"
      kubectl -n ate-system delete ds image-plugin-nydus-dragonfly image-plugin-nydus-dragonfly-seed --ignore-not-found --wait=true >/dev/null 2>&1 || true
      kubectl apply -f "${nydus_yaml}"
      kubectl -n ate-system rollout status deploy/dragonfly-mysql deploy/dragonfly-redis --timeout=10m
      kubectl -n ate-system rollout status deploy/dragonfly-manager deploy/dragonfly-scheduler --timeout=10m
      kubectl -n ate-system rollout status ds/nydus-blob-seed --timeout=10m
      kubectl -n ate-system rollout status ds/image-plugin-nydus-dragonfly --timeout=15m
      ;;
    *)
      echo "Unknown arm: ${arm}" >&2
      exit 1
      ;;
  esac
}

run_loader_job() {
  local job="$1" mode="$2" arm="$3" atespace="$4" extra_args="$5" out_dir="$6"
  mkdir -p "${out_dir}"
  kubectl -n imgbench delete job "${job}" --ignore-not-found --wait=true >/dev/null 2>&1 || true

  local extra=""
  # shellcheck disable=SC2086
  for tok in ${extra_args}; do
    extra+=", \"${tok}\""
  done

  # shellcheck disable=SC2016
  JOB="${job}" MODE="${mode}" ARM="${arm}" ATESPACE="${atespace}" SNAPSHOT_LOCATION="${SNAPSHOT_LOCATION}" EXTRA_ARGS="${extra}" \
    envsubst '${JOB} ${MODE} ${ARM} ${ATESPACE} ${SNAPSHOT_LOCATION} ${EXTRA_ARGS}' < "${SCRIPT_DIR}/oci-loader-job.yaml.tmpl" > "${out_dir}/job.yaml"
  "${KO}" resolve --base-import-paths -f "${out_dir}/job.yaml" 2>/dev/null > "${out_dir}/job.resolved.yaml"
  kubectl apply -f "${out_dir}/job.resolved.yaml"
  kubectl -n imgbench wait --for=condition=Ready pod -l "job-name=${job}" --timeout=10m
  local pod
  pod="$(kubectl -n imgbench get pods -l "job-name=${job}" -o jsonpath='{.items[0].metadata.name}')"
  echo "[${job}] loader pod ${pod} running..."

  local port=$((19000 + RANDOM % 1000)) pf
  while :; do
    kubectl -n imgbench port-forward "pod/${pod}" "${port}:8080" >/dev/null 2>&1 &
    pf=$!
    sleep 2
    curl -sf "http://127.0.0.1:${port}/out.jsonl" -o "${out_dir}/turns.jsonl.part" 2>/dev/null && mv "${out_dir}/turns.jsonl.part" "${out_dir}/turns.jsonl" || true
    if curl -sf "http://127.0.0.1:${port}/done" -o "${out_dir}/done" 2>/dev/null; then
      kill "${pf}" 2>/dev/null || true
      break
    fi
    kill "${pf}" 2>/dev/null || true
    if ! kubectl -n imgbench get pod "${pod}" >/dev/null 2>&1; then
      echo "WARN: pod ${pod} disappeared" >&2
      break
    fi
    sleep 5
  done
  kubectl -n imgbench logs "${pod}" > "${out_dir}/loader.log" 2>&1 || true
  kubectl -n imgbench delete job "${job}" --wait=false >/dev/null 2>&1 || true
  echo "[${job}] finished with code $(cat "${out_dir}/done" 2>/dev/null || echo '?')"
}

case "${cmd}" in
  setup)
    gcloud compute scp "${SCRIPT_DIR}/reset-cold-node.sh" "${UBUNTU_NODE}:/tmp/reset-cold-node.sh" \
      --project="${PROJECT_ID}" --zone="${CLUSTER_LOCATION}" --quiet
    gcloud compute ssh "${UBUNTU_NODE}" --project="${PROJECT_ID}" --zone="${CLUSTER_LOCATION}" --quiet \
      --command="sudo mv -f /tmp/reset-cold-node.sh /usr/local/bin/reset-cold-node.sh && sudo chmod 755 /usr/local/bin/reset-cold-node.sh"
    ;;

  switch-plugin)
    switch_plugin "$1"
    ;;

  bake)
    flavor="${1:?usage: bake oci|nydus}"
    if [[ "${flavor}" == "oci" ]]; then
      switch_plugin glovebox-shared-erofs
      run_loader_job "bake-oci-16" "bake" "glovebox-shared-erofs" "imgbench-oci-16" "-images /config/images-16-bench.jsonl" "${RESULTS_ROOT}/bake-imgbench-oci-16"
      run_loader_job "bake-oci-rl1" "bake" "glovebox-shared-erofs" "imgbench-oci-rl1" "-images /config/images-1-shared-rl.jsonl" "${RESULTS_ROOT}/bake-imgbench-oci-rl1"
    else
      switch_plugin nydus
      run_loader_job "bake-nydus-16" "bake" "nydus" "imgbench-nydus-16" "-images /config/images-16-bench.jsonl -nydus" "${RESULTS_ROOT}/bake-imgbench-nydus-16"
      run_loader_job "bake-nydus-rl1" "bake" "nydus" "imgbench-nydus-rl1" "-images /config/images-1-shared-rl.jsonl -nydus" "${RESULTS_ROOT}/bake-imgbench-nydus-rl1"
    fi
    ;;

  run-arm)
    arm="$1"       # pull | nydus | glovebox-shared-ro | glovebox-shared-erofs | glovebox-per-actor-cow
    scenario="$2"  # scenario-a | scenario-b
    rep="${3:-1}"
    out_dir="${RESULTS_ROOT}/${scenario}-${arm}-rep${rep}"
    mkdir -p "${out_dir}"

    gcloud compute scp "${SCRIPT_DIR}/reset-cold-node.sh" "${UBUNTU_NODE}:/tmp/reset-cold-node.sh" \
      --project="${PROJECT_ID}" --zone="${CLUSTER_LOCATION}" --quiet
    gcloud compute ssh "${UBUNTU_NODE}" --project="${PROJECT_ID}" --zone="${CLUSTER_LOCATION}" --quiet \
      --command="sudo mv -f /tmp/reset-cold-node.sh /usr/local/bin/reset-cold-node.sh && sudo chmod 755 /usr/local/bin/reset-cold-node.sh"

    switch_plugin "${arm}"
    echo "=== Resetting cold node state on ${UBUNTU_NODE} ==="
    gcloud compute ssh "${UBUNTU_NODE}" --project="${PROJECT_ID}" --zone="${CLUSTER_LOCATION}" --quiet \
      --command="sudo /usr/local/bin/reset-cold-node.sh" | tee "${out_dir}/node-memory.jsonl"

    if [[ "${scenario}" == "scenario-a" ]]; then
      img_flag="/config/images-16-bench.jsonl"
      actors=16
      aspace="imgbench-oci-16"
      [[ "${arm}" == "nydus" ]] && aspace="imgbench-nydus-16"
    else
      img_flag="/config/images-1-shared-rl.jsonl"
      actors=24
      aspace="imgbench-oci-rl1"
      [[ "${arm}" == "nydus" ]] && aspace="imgbench-nydus-rl1"
    fi

    start_ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    gcloud compute ssh "${UBUNTU_NODE}" --project="${PROJECT_ID}" --zone="${CLUSTER_LOCATION}" --ssh-flag="-n" --quiet --command="rm -f /tmp/stop-oci-sampler" || true
    gcloud compute ssh "${UBUNTU_NODE}" --project="${PROJECT_ID}" --zone="${CLUSTER_LOCATION}" --ssh-flag="-n" --quiet \
      --command="for _ in \$(seq 1 900); do
        [ -f /tmp/stop-oci-sampler ] && break
        ts=\$(date -u +%Y-%m-%dT%H:%M:%SZ)
        ma=\$(awk '/^MemAvailable:/ {print \$2}' /proc/meminfo)
        ca=\$(awk '/^Cached:/ {print \$2}' /proc/meminfo)
        bm=\$(grep -c '/var/lib/ate/image-plugin-cache/glovebox/' /proc/mounts || true)
        em=\$(awk '\$3==\"erofs\" {c++} END {print c+0}' /proc/mounts)
        zp=\$(pgrep -f 'vduse-blk-zbd-daemon' | head -n1 || true)
        zr=0
        [ -n \"\${zp}\" ] && [ -r \"/proc/\${zp}/status\" ] && zr=\$(awk '/^VmRSS:/ {print \$2}' \"/proc/\${zp}/status\")
        echo \"{\\\"ts\\\":\\\"\${ts}\\\",\\\"mem_available_kb\\\":\${ma},\\\"cached_kb\\\":\${ca},\\\"glovebox_mounts\\\":\${bm},\\\"erofs_mounts\\\":\${em},\\\"zbd_daemon_rss_kb\\\":\${zr}}\"
        sleep 1
      done" >> "${out_dir}/node-memory.jsonl" 2>/dev/null &
    sampler_pid=$!

    run_loader_job "run-${scenario}-${arm}-r${rep}" "run" "${arm}" "${aspace}" "-rep ${rep} -images ${img_flag} -actors ${actors} -turns 2" "${out_dir}"

    gcloud compute ssh "${UBUNTU_NODE}" --project="${PROJECT_ID}" --zone="${CLUSTER_LOCATION}" --quiet --command="touch /tmp/stop-oci-sampler" || true
    wait "${sampler_pid}" 2>/dev/null || true

    atelet_pod="$(kubectl -n ate-system get pods -l app=atelet --field-selector "spec.nodeName=${UBUNTU_NODE}" -o jsonpath='{.items[0].metadata.name}')"
    kubectl -n ate-system logs "${atelet_pod}" -c atelet --since-time="${start_ts}" > "${out_dir}/atelet.log" 2>&1 || true

    if [[ "${arm}" == glovebox-* ]]; then
      gb_pod="$(kubectl -n ate-system get pods -l ate.dev/image-plugin=glovebox --field-selector "spec.nodeName=${UBUNTU_NODE}" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
      if [[ -n "${gb_pod}" ]]; then
        kubectl -n ate-system logs "${gb_pod}" --since-time="${start_ts}" > "${out_dir}/glovebox-plugin.log" 2>&1 || true
      fi
    fi

    worker_pods="$(kubectl -n imgbench get pods -l workload=bench-oci-uvm -o jsonpath='{.items[*].metadata.name}')"
    : > "${out_dir}/workers-restore.log"
    for wp in ${worker_pods}; do
      kubectl -n imgbench logs "${wp}" --since-time="${start_ts}" >> "${out_dir}/workers-restore.log" 2>&1 || true
    done

    echo "=== Summary of ${out_dir}/turns.jsonl ==="
    wc -l "${out_dir}/turns.jsonl"
    grep -c '"error"' "${out_dir}/turns.jsonl" || true
    ;;
esac
