#!/bin/bash
set -e

case "$1" in
  generator)
    shift
    exec python data_generator/alt_generator.py "$@"
    ;;
  sparkjob)
    shift
    exec bash utils/run-silver-pipeline.sh "$@"
    ;;
  *)
    echo "Usage: $0 {generator|sparkjob} [args...]"
    exit 1
    ;;
esac
