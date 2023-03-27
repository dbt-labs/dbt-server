#!/bin/bash
celery -A dbt_worker.app worker --loglevel=INFO --detach
