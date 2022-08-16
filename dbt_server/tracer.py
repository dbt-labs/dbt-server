import os

APM_ENABLED = os.getenv("APPLICATION_TRACING_ENABLED", "")

ENV_HAS_DDTRACE = False
TRACING_ENABLED = False

try:
    import ddtrace

    ENV_HAS_DDTRACE = True
except (ModuleNotFoundError, ImportError):
    pass


def setup_tracing():
    global TRACING_ENABLED

    if ENV_HAS_DDTRACE and APM_ENABLED:
        TRACING_ENABLED = True
        ddtrace.patch_all()


setup_tracing()
