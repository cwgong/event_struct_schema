#!/bin/sh
cd /data/workflow-eventdiscovery-service

LOGS_DIR=logs
if [ ! -d "${LOGS_DIR}" ]
then
  mkdir "${LOGS_DIR}"
fi

python3 workflow-eventdiscovery-service.py

echo "workflow-eventdiscovery-service starting..."
