# Need to run this as early in the server's startup as possible
from dbt_server import tracer  # noqa

from dbt_server import models
from dbt_server.database import engine
from dbt_server.services import dbt_service, filesystem_service
from dbt_server.views import app
from dbt_server.logging import GLOBAL_LOGGER as logger, configure_uvicorn_access_log
from dbt_server.state import LAST_PARSED
from dbt_server.exceptions import StateNotFoundException


models.Base.metadata.create_all(bind=engine)
dbt_service.disable_tracking()


def startup_cache_initialize():
    """
    Initialize the manifest cache at startup. The cache will only be populated if there is
    a latest-state-id.txt file pointing to a state folder with a pre-compiled manifest.
    If any step fails (the latest-state-id.txt file is missing, there's no compiled manifest,
    or it can't be deserialized) then continue without caching.
    """

    # If an exception is raised in this method, the dbt-server will fail to start up.
    # Be careful here :)

    latest_state_id = filesystem_service.get_latest_state_id(None)
    if latest_state_id is None:
        logger.info("[STARTUP] No latest state found - not loading manifest into cache")
        return

    manifest_path = filesystem_service.get_path(latest_state_id, "manifest.msgpack")
    logger.info(
        f"[STARTUP] Loading manifest from file system (state_id={latest_state_id})"
    )

    try:
        manifest = dbt_service.deserialize_manifest(manifest_path)
    except (TypeError, ValueError) as e:
        logger.error(f"[STARTUP] Could not deserialize manifest: {str(e)}")
        return
    except (StateNotFoundException):
        logger.error(
            f"[STARTUP] Specified latest state not found - not loading manifest (state_id={latest_state_id})"
        )
        return

    LAST_PARSED.set_last_parsed_manifest(latest_state_id, manifest)
    logger.info(f"[STARTUP] Cached manifest in memory (state_id={latest_state_id})")


@app.on_event("startup")
async def startup_event():
    # This method is `async` intentionally to block serving until startup is complete

    configure_uvicorn_access_log()
    startup_cache_initialize()
    dbt_service.inject_dd_trace_into_core_lib()
