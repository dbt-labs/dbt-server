
from dbt_rpc.logging import GLOBAL_LOGGER as logger, CapturingLogManager
from contextlib import contextmanager, ContextDecorator


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
