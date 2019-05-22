#!/usr/bin/env bash
export EXEC="/home/evan/CLionProjects/aligner_cache/cmake-build-release/aligner_cache"
$EXEC --from_config ../etc/baseline.ini &> baseline_run.txt
$EXEC --from_config ../etc/lru_only.ini &> lru_only_run.txt
$EXEC --from_config ../etc/lru_full.ini &> lru_full_run.txt
$EXEC --from_config ../etc/mru_full.ini &> mru_full_run.txt