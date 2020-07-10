#!/bin/sh
cd /data/eventengine-docfeaturextract-service
LOGS_DIR=logs

if [ ! -d "${LOGS_DIR}" ]
then
  mkdir "${LOGS_DIR}"
fi

python3 eventengine-docfeaturextract-service.py eventengine-docfeaturextract-service.conf

echo "eventengine-docfeaturextract-service.py starting..."
