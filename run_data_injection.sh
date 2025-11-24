#!/bin/bash

PROJECT_ROOT="/Users/amohiuddeen/Github/dq-framework-poc"
cd "$PROJECT_ROOT"

BATCH_TIME=""
if [ "$1" != "" ]; then
    BATCH_TIME="$1"
fi

echo "======================================================================"
echo "📊 Data Injection Script"
echo "======================================================================"

if [ "$BATCH_TIME" != "" ]; then
    echo "Batch Time: $BATCH_TIME"
    python3 scripts/generate_hourly_data.py "$BATCH_TIME"
else
    echo "Batch Time: Current hour (auto)"
    python3 scripts/generate_hourly_data.py
fi

echo ""
echo "✅ Data injection complete!"
echo ""
echo "To validate data:"
echo "  airflow dags trigger dq_hourly_pipeline_bash"
echo "======================================================================"
