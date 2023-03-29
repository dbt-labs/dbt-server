#!/bin/bash
# If set to true, ddtrace-run will be enabled for dbt-server.
readonly dbt_server_enable_ddtrace="${DBT_SERVER_ENABLE_DDTRACE-false}"
# How many dbt server worker processes.
readonly dbt_server_worker="${DBT_SERVER_WORKER_NUM-3}"
# Dbt server listening port.
readonly dbt_server_port="${DBT_SERVER_WORKER_PORT-8585}"
# Max # of requests that dbt server is allowed.
readonly dbt_server_max_requests="${DBT_SERVER_MAX_REQUESTS-5}"

gunicorn="gunicorn"
if [ "${dbt_server_enable_ddtrace}" = "true" ]; then
    dd_trace="ddtrace-run gunicorn"
fi

service redis-server start
service celeryd start

${gunicorn} dbt_server.server:app --workers ${dbt_server_worker} \
--worker-class uvicorn.workers.UvicornWorker --bind 0.0.0.0:${dbt_server_port} \
--max-requests ${dbt_server_max_requests} --max-requests-jitter 3
