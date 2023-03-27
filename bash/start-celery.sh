#!/bin/bash
# TODO: Detach mode used here can't be easily stopped or restarted, we may 
# consider use init.d for better maintenance.
celery -A dbt_worker.app worker --loglevel=INFO --detach
