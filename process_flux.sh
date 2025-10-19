#!/bin/bash

# Exit if missing arguments
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <month> <year>"
    exit 1
fi

MONTH=$1
YEAR=$2

# Fixed paths
PYTHON_SCRIPT="estimate_flux.py"
PROJECT_DIR="/corral/utexas/CDA23001/water/eddycov"
INPUT_FILE="software/eddyflow/EddyPro/templates/project.eddypro"
META_FILE="software/eddyflow/EddyPro/setx.metadata"
RAW_DIR="decoded_data/ts_data"
OUTPUT_DIR="flux_data"

# Run the Python command to create eddypro file
conda activate geos
python "$PYTHON_SCRIPT" \
    -m "$MONTH" \
    -y "$YEAR" \
    -p "$PROJECT_DIR" \
    -i "$INPUT_FILE" \
    -f "$META_FILE" \
    -r "$RAW_DIR" \
    -o "$OUTPUT_DIR"
