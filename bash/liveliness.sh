#!/bin/bash

# First, make a call to the ready server endpoint. It should return 200
response=$(curl -s -o /dev/null -w "%{http_code}" localhost:8585/ready)

# Check if the response status code is 200
if [ "$response" -eq 200 ]; then
  # Set default value for CELERY_BROKER_URL if not already set
  CELERY_BROKER_URL=${CELERY_BROKER_URL:-redis://localhost:6379/0}

  # Check if the broker is reachable
  if redis-cli -u "$CELERY_BROKER_URL" PING > /dev/null 2>&1; then
    # Check the time difference between the current timestamp and the last modification timestamp
    # of the worker_heartbeat file. This will also eval to False if the file doesn't exist yet.
    if test $(($(date +%s) - $(stat -c %Y /tmp/worker_heartbeat))) -lt 10; then
      # Both checks passed, return 1
      exit 1
    fi
  fi
fi

# Either of the checks failed, return 0
exit 0
