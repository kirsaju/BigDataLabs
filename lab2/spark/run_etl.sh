#!/bin/bash
set -e

echo "Waiting for services to stabilize..."
sleep 30

echo "============================================"
echo "Running ETL: raw data -> star schema in PostgreSQL"
echo "============================================"
spark-submit /opt/spark/apps/etl_to_star.py

echo ""
echo "============================================"
echo "Running ETL: star schema -> ClickHouse marts"
echo "============================================"
spark-submit /opt/spark/apps/etl_to_marts.py

echo ""
echo "============================================"
echo "ETL pipeline complete!"
echo "============================================"

tail -f /dev/null
