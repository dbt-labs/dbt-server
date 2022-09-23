# Need to run this as early in the server's startup as possible
from . import tracer  # noqa

import asyncio
import signal

from . import models
from .database import engine
from .services import dbt_service
from .views import ALLOW_ORCHESTRATED_SHUTDOWN
from .views import app

# Where... does this actually go?
# And what the heck do we do about migrations?
models.Base.metadata.create_all(bind=engine)

# TODO : This messes with stuff
dbt_service.disable_tracking()


@app.on_event("startup")
async def startup_event():
    # avoid circular import
    from .logging import configure_uvicorn_access_log

    if ALLOW_ORCHESTRATED_SHUTDOWN:
        override_signal_handlers()

    configure_uvicorn_access_log()


@app.on_event("shutdown")
def shutdown_event():
    pass


def override_signal_handlers():
    # avoid circular import
    from .logging import GLOBAL_LOGGER as logger

    logger.info("Setting up signal handling....")
    block_count = 0

    def handle_exit(signum, frame):
        nonlocal block_count
        if block_count > 0:
            logger.info(
                "Received multiple SIGINT or SIGTERM signals, "
                "calling the original signal handler.",
            )
            if signum == signal.SIGINT and original_sigint_handler:
                original_sigint_handler._run()
            elif signum == signal.SIGTERM and original_sigterm_handler:
                original_sigterm_handler._run()
        else:
            logger.info("Press CTRL+C again to quit.")
            block_count += 1

    loop = asyncio.get_running_loop()
    original_sigint_handler = loop._signal_handlers.get(signal.SIGINT)
    original_sigterm_handler = loop._signal_handlers.get(signal.SIGTERM)
    loop.remove_signal_handler(signal.SIGINT)
    loop.remove_signal_handler(signal.SIGTERM)
    loop.add_signal_handler(signal.SIGINT, handle_exit, signal.SIGINT, None)
    loop.add_signal_handler(signal.SIGTERM, handle_exit, signal.SIGTERM, None)
