#!/usr/bin/env bash
#
# End-to-end benchmark runner for the WEKA fork of bazel-remote.
#
# Sequence:
#   1. Generate a synthetic cache of N files (unless ./cache-data exists and is
#      the right size — set REGEN=1 to force regeneration).
#   2. Start leader + 2 followers via docker-compose against the chosen image.
#   3. Measure boot time (leader Ready) and first GC scan cycle completion time.
#   4. Print report + tear down.
#
# Usage:
#     ./run_bench.sh <image-tag> [num-files]
#
# Environment overrides:
#     NUM_FILES  (default: 100000)         file count for synthetic cache
#     REGEN=1                              force cache regeneration
#     KEEP_UP=1                            skip teardown at end
#     NO_BUILD=1                           skip `docker compose build`
#                                          (assumes the tag already exists locally)
#
# Compose builds the image from ../Dockerfile using the current working-tree
# source and tags it with the value of BAZEL_REMOTE_IMAGE. To A/B compare:
#     git checkout master && ./run_bench.sh bazel-remote:baseline
#     git checkout v19-scan && ./run_bench.sh bazel-remote:scan-parallel
#
# The reported numbers can be diffed between two runs to attribute improvements.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

IMAGE="${1:?Usage: run_bench.sh <image-tag> [num-files]}"
NUM_FILES="${2:-${NUM_FILES:-100000}}"
REGEN="${REGEN:-0}"
KEEP_UP="${KEEP_UP:-0}"
NO_BUILD="${NO_BUILD:-0}"

CACHE_DIR="./cache-data"
LOG_DIR="./bench-logs"
mkdir -p "${LOG_DIR}"

log() { echo "[$(date +%H:%M:%S)] $*"; }

# ----------------------------------------------------------------------
# 1. Cache generation
# ----------------------------------------------------------------------
if [[ "${REGEN}" == "1" ]] || [[ ! -d "${CACHE_DIR}/cas.v2" ]]; then
  log "Generating synthetic cache with ${NUM_FILES} files (REGEN=${REGEN})"
  rm -rf "${CACHE_DIR}"
  mkdir -p "${CACHE_DIR}"
  time python3 gen_cache.py "${CACHE_DIR}" "${NUM_FILES}"
else
  existing="$(find "${CACHE_DIR}/cas.v2" -type f 2>/dev/null | wc -l | tr -d ' ')"
  log "Reusing existing ${CACHE_DIR} (${existing} files). Set REGEN=1 to force regeneration."
fi

# ----------------------------------------------------------------------
# 2. Build (unless NO_BUILD=1) + boot the stack
# ----------------------------------------------------------------------
export BAZEL_REMOTE_IMAGE="${IMAGE}"
docker compose down --volumes --remove-orphans >/dev/null 2>&1 || true

if [[ "${NO_BUILD}" != "1" ]]; then
  log "Building image ${IMAGE} from ../Dockerfile"
  docker compose build 2>&1 | tail -20
fi

log "Booting bazel-remote stack with image=${IMAGE}"
BOOT_T0="$(date +%s)"
docker compose up -d

# ----------------------------------------------------------------------
# 3. Measure — leader Ready
# ----------------------------------------------------------------------
log "Waiting for leader HTTP /status to answer (proxy for LRU load complete)"
READY_TIMEOUT=1800  # 30 min hard cap for local testing
while true; do
  if curl -sf http://localhost:8080/status >/dev/null 2>&1; then
    break
  fi
  elapsed=$(($(date +%s) - BOOT_T0))
  if (( elapsed > READY_TIMEOUT )); then
    log "TIMEOUT (${READY_TIMEOUT}s) waiting for leader Ready — dumping logs"
    docker compose logs leader --tail=100 | tee "${LOG_DIR}/leader-boot-timeout.log"
    break
  fi
  sleep 2
done
BOOT_TIME=$(($(date +%s) - BOOT_T0))
log "Leader Ready after ${BOOT_TIME}s"

# ----------------------------------------------------------------------
# 4. Measure — first GC scan cycle completion
# ----------------------------------------------------------------------
log "Waiting for GC 'Shared storage GC: current size' log line (indicates scanFilesWithAtime completed)"
GC_T0="$(date +%s)"
GC_TIMEOUT=1800  # 30 min hard cap
GC_SCAN_TIME=""
while true; do
  if docker compose logs leader 2>&1 | grep -q "Shared storage GC: current size"; then
    GC_SCAN_TIME=$(($(date +%s) - GC_T0))
    break
  fi
  elapsed=$(($(date +%s) - GC_T0))
  if (( elapsed > GC_TIMEOUT )); then
    log "TIMEOUT (${GC_TIMEOUT}s) waiting for GC cycle"
    break
  fi
  sleep 2
done

# ----------------------------------------------------------------------
# 5. Optional — wait for "freed" line
# ----------------------------------------------------------------------
FREED_TIME=""
if [[ -n "${GC_SCAN_TIME}" ]]; then
  log "GC scan produced 'exceeds limit' log line. Waiting for 'freed' completion."
  FREE_T0="$(date +%s)"
  while (( $(date +%s) - FREE_T0 < 600 )); do
    if docker compose logs leader 2>&1 | grep -q "freed"; then
      FREED_TIME=$(($(date +%s) - FREE_T0))
      break
    fi
    sleep 2
  done
fi

# ----------------------------------------------------------------------
# 6. Persist logs
# ----------------------------------------------------------------------
TAG_SAFE="$(echo "${IMAGE}" | tr '/:' '__')"
STAMP="$(date +%Y%m%d-%H%M%S)"
LOG_PREFIX="${LOG_DIR}/${STAMP}-${TAG_SAFE}"
docker compose logs leader > "${LOG_PREFIX}.leader.log" 2>&1
docker compose logs follower-1 > "${LOG_PREFIX}.follower-1.log" 2>&1
docker compose logs follower-2 > "${LOG_PREFIX}.follower-2.log" 2>&1

# ----------------------------------------------------------------------
# 7. Report
# ----------------------------------------------------------------------
echo ""
echo "======================================================================="
echo "  Benchmark result — image=${IMAGE}"
echo "  files=${NUM_FILES}"
echo "-----------------------------------------------------------------------"
printf "  boot_time                   : %s\n" "${BOOT_TIME}s"
printf "  first_gc_scan_completion    : %s\n" "${GC_SCAN_TIME:-TIMEOUT}"
printf "  eviction_phase_duration     : %s\n" "${FREED_TIME:-unmeasured}"
echo "  logs saved to               : ${LOG_PREFIX}.*.log"
echo "======================================================================="
echo ""

# ----------------------------------------------------------------------
# 8. Teardown
# ----------------------------------------------------------------------
if [[ "${KEEP_UP}" == "1" ]]; then
  log "KEEP_UP=1 set — stack left running. Tear down with: docker compose down --volumes"
else
  log "Tearing down"
  docker compose down --volumes >/dev/null 2>&1
fi
