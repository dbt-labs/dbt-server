from pathlib import Path
from celery import Celery, bootsteps
from celery.signals import worker_ready, worker_shutdown
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

LIVENESS_FILE = Path("/tmp/worker_heartbeat")


class LivenessProbe(bootsteps.StartStopStep):
    requires = {"celery.worker.components:Timer"}

    def __init__(self, _, **kwargs):
        self.requests = []
        self.tref = None

    def start(self, worker):
        self.tref = worker.timer.call_repeatedly(
            1.0,
            self.update_heartbeat_file,
            (worker,),
            priority=10,
        )

    def stop(self, _):
        LIVENESS_FILE.unlink(missing_ok=True)

    def update_heartbeat_file(self, _):
        LIVENESS_FILE.touch()


@worker_ready.connect
def worker_ready(**_):
    LIVENESS_FILE.touch()


@worker_shutdown.connect
def worker_shutdown(**_):
    LIVENESS_FILE.unlink(missing_ok=True)


app.steps["worker"].add(LivenessProbe)

if __name__ == "__main__":
    app.start()
