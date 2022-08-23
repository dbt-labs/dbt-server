import os

APM_ENABLED = os.getenv("APPLICATION_TRACING_ENABLED", "") not in (
    "",
    "0",
    "f",
    "false",
)

ENV_HAS_DDTRACE = False
TRACING_ENABLED = False

try:
    import ddtrace

    ENV_HAS_DDTRACE = True

    # Importing this kicks of tracing. Only import
    # it if APM_ENABLED is truthy
    if APM_ENABLED:
        import ddtrace.profiling.auto

except (ModuleNotFoundError, ImportError):
    pass


def setup_tracing():
    global TRACING_ENABLED

    if ENV_HAS_DDTRACE and APM_ENABLED:
        TRACING_ENABLED = True
        ddtrace.patch(logging=True)
        ddtrace.patch_all()


def wrap(func):
    """
    Decorator that adds datadog tracing if ddtrace
    is installed and enabled, otherwise it's a no-op
    """

    def no_op(*args, **kwargs):
        return func(*args, **kwargs)

    def dd_trace(*args, **kwargs):
        name = f"{func.__module__}.{func.__name__}"
        with ddtrace.tracer.trace(name):
            return func(*args, **kwargs)

    if ENV_HAS_DDTRACE and APM_ENABLED:
        return dd_trace
    else:
        return no_op


setup_tracing()
