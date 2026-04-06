#!/bin/bash
set -e

echo "============================================"
echo "Pre-processing: Preparing raw CSVs for PostgreSQL"
echo "============================================"

pip install -r requirements.txt --quiet

python scripts/preprocess.py --data-dir data

echo "Pre-processing finished successfully."
