#!/usr/bin/env bash
# PYTHONPATH: arrel del repo + dags (Clients). Client Spark: paquet `spark_client/` a l'arrel del repo.
set -euo pipefail

ROOT="${BDM_REPO_ROOT:-${SPARK_WORK_DIR:-/opt/spark/work-dir}}"
ROOT="${ROOT%/}"

if [[ ! -d "${ROOT}/spark_client" ]]; then
  echo "ERROR: no existe ${ROOT}/spark_client (repositori incomplet o BDM_REPO_ROOT incorrecte)." >&2
  exit 1
fi

if [[ -d "${ROOT}/dags" ]]; then
  if [[ -n "${PYTHONPATH:-}" ]]; then
    export PYTHONPATH="${ROOT}:${ROOT}/dags:${PYTHONPATH}"
  else
    export PYTHONPATH="${ROOT}:${ROOT}/dags"
  fi
else
  if [[ -n "${PYTHONPATH:-}" ]]; then
    export PYTHONPATH="${ROOT}:${PYTHONPATH}"
  else
    export PYTHONPATH="${ROOT}"
  fi
fi

exec /opt/spark/bin/spark-submit \
  --master "${SPARK_MASTER:-spark://spark-master:7077}" \
  "${ROOT}/trusted-zone/trusted_zone.py" \
  "$@"
