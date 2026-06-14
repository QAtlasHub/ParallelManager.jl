#!/bin/bash
#==============================================================================
# examples/batch/submit_slurm.sh — HPC submission template.
#
# This is the bash half of the stack. It does three things the Julia side
# cannot: allocate the nodes, export the env vars init_workers! reads, and
# wire the graceful-stop signal. The Julia side stays identical to the local
# run — `init_workers!(mode=:auto)` detects SLURM_JOB_ID and fans out.
#
#   sbatch examples/batch/submit_slurm.sh examples/configs/logistic.toml
#
# (The toy logistic sweep does NOT need a cluster — this file is the template
#  you copy for a real DMRG/Monte-Carlo workload.)
#==============================================================================
#SBATCH --job-name=pm-example
#SBATCH --nodes=1
#SBATCH --ntasks=4               # 1 master + 3 workers (master is one task)
#SBATCH --cpus-per-task=4        # CPUs (BLAS lane) per worker
#SBATCH --time=01:00:00
#SBATCH --signal=B:USR1@60       # send SIGUSR1 to the batch shell 60 s before the kill

set -euo pipefail
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"   # ParallelManager.jl root
CONFIG="${1:-${PROJECT_DIR}/examples/configs/logistic.toml}"
COMPUTE="${PROJECT_DIR}/examples/scripts/compute.jl"
JULIA_BIN="${JULIA_BIN:-julia}"

# ── env vars that ParallelManager.init_workers!(mode=:slurm) reads ──
export JULIA_SLURM_N_WORKERS=$(( SLURM_NTASKS - 1 ))   # master is one of the tasks
export JULIA_WORKER_CPUS=${SLURM_CPUS_PER_TASK}        # per-worker BLAS threads
export JULIA_WORKER_TIMEOUT=300                        # cold-NFS handshake window
export JULIA_NUM_THREADS=1
# Keep BLAS single-threaded per process — multi-thread BLAS in multi-process
# Julia is a classic OpenBLAS segfault source. init_workers! also enforces this.
export OPENBLAS_NUM_THREADS=${SLURM_CPUS_PER_TASK}
export MKL_NUM_THREADS=${SLURM_CPUS_PER_TASK}

# ── where the data goes: scratch on HPC, rsync'd back to the local out/ later ──
export DATAVAULT_OUTDIR="${DATAVAULT_OUTDIR:-${PROJECT_DIR}/examples/out}"

# ── graceful stop: trap SIGUSR1 → touch the sentinel compute.jl watches via
#    RunOpts(stop_flag=…). run!/run_loop! finish the in-flight key and exit. ──
export PM_STOP_FLAG="${DATAVAULT_OUTDIR}/STOP_NOW_${SLURM_JOB_ID}"
trap 'touch "${PM_STOP_FLAG}"' USR1

# Pin the master to core 0 so SlurmClusterManager's internal srun can launch
# the workers across the allocation without nested-job-step contention.
taskset -c 0 "${JULIA_BIN}" --project="${PROJECT_DIR}" --threads=1 "${COMPUTE}" "${CONFIG}" &
CHILD_PID=$!
wait "${CHILD_PID}"
