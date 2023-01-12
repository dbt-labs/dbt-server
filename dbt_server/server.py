# Need to run this as early in the server's startup as possible
import os
from typing import Optional
from pydantic import BaseModel
from dbt_server import tracer  # noqa

from dbt_server import models
from dbt_server.database import engine
from dbt_server.services import dbt_service, filesystem_service
from dbt_server.views import app
from dbt_server.logging import DBT_SERVER_LOGGER as logger, configure_uvicorn_access_log
from dbt_server.state import LAST_PARSED
from dbt_server.exceptions import StateNotFoundException


models.Base.metadata.create_all(bind=engine)
dbt_service.disable_tracking()


class ConfigArgs(BaseModel):
    target: Optional[str] = None
    profile: Optional[str] = None


def startup_cache_initialize():
    """
    Initialize the manifest cache at startup. The cache will only be populated if there is
    a latest-state-id.txt file pointing to a state folder with a pre-compiled manifest.
    If any step fails (the latest-state-id.txt file is missing, there's no compiled manifest,
    or it can't be deserialized) then continue without caching.
    
    TODO: This only checks for an existing state_id, we don't store the last project_path on
    disk. Need to follow up to see if that would be beneficial.
    """

    # If an exception is raised in this method, the dbt-server will fail to start up.
    # Be careful here :)
    latest_state_id = filesystem_service.get_latest_state_id(None)
    if latest_state_id is None:
        logger.info("[STARTUP] No latest state found - not loading manifest into cache")
        return

    root_path = filesystem_service.get_root_path(latest_state_id)
    manifest_path = filesystem_service.get_path(root_path, "manifest.msgpack")
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

    target_name = os.environ.get("__DBT_TARGET_NAME", None)
    if target_name == "":
        target_name = None
    config_args = ConfigArgs(target=target_name)

    root_path = filesystem_service.get_root_path(latest_state_id)
    manifest_size = filesystem_service.get_size(manifest_path)
    config = dbt_service.create_dbt_config(root_path, config_args)

    LAST_PARSED.set_last_parsed_manifest(
        latest_state_id, root_path, manifest, manifest_size, config
    )

    logger.info(f"[STARTUP] Cached manifest in memory (state_id={latest_state_id})")


@tracer.wrap
@app.on_event("startup")
async def startup_event():
    # This method is `async` intentionally to block serving until startup is complete
    configure_uvicorn_access_log()
    dbt_service.inject_dd_trace_into_core_lib()
    startup_cache_initialize()
