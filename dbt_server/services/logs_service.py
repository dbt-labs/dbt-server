from dbt_server.logging import CapturingLogManager
from contextlib import contextmanager


@contextmanager
def capture_logs(log_path, logs):
    """
    This captures logs from the yielded (presumably dbt)
    invocation and appends them to the `logs` arg
    """
    log_manager = CapturingLogManager(log_path)
    log_manager.setup_handlers()
    try:
        yield

    finally:
        logs += log_manager.getLogs()
        log_manager.cleanup()
