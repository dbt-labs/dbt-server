import os
from dbt import version

APM_ENABLED = os.getenv("APPLICATION_TRACING_ENABLED", "") not in (
    "",
    "0",
    "f",
    "false",
)

ENV_HAS_DDTRACE = False
TRACING_ENABLED = False
DBT_VERSION = str(version.installed).lstrip("=")

try:
    import ddtrace
    import ddtrace.profiling.auto

    ENV_HAS_DDTRACE = True
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
        try:
            # Should contain only one adapter
            adapter_map = {k: v for k, v in version._get_dbt_plugins_info()}
            adapter_name = list(adapter_map.keys())[0]
            adapter_version = adapter_map[adapter_name]
        # In case this private method is changed
        except AttributeError:
            adapter_name = ""
            adapter_version = ""

        with ddtrace.tracer.trace(name) as span:
            span.set_tag("dbt.version", DBT_VERSION)
            span.set_tag(f"dbt.adapters.{adapter_name}", adapter_version)
            return func(*args, **kwargs)

    if ENV_HAS_DDTRACE and APM_ENABLED:
        return dd_trace
    else:
        return no_op


setup_tracing()
