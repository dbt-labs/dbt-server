from celery import Celery
from dbt_server.flags import CELERY_BACKEND_URL
from dbt_server.flags import CELERY_BROKER_URL
from dbt_worker import celeryconfig

# Example run:
#   celery -A dbt_worker.app worker --concurrency=1 --loglevel=INFO
app = Celery(
    "dbt-worker",
    broker=CELERY_BROKER_URL.get(),
    backend=CELERY_BACKEND_URL.get(),
    # Includes those file for worker process, otherwise worker process can't
    # find task definition.
    include=["dbt_worker.tasks"],
)

app.config_from_object(celeryconfig)

if __name__ == "__main__":
    app.start()
