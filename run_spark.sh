#!/bin/bash
cd /app
echo "Running spark script at $(date)" >> /app/cron.log
python3 /app/jsonConvertToSpark.py >> /app/cron.log 2>&1
