# Need to run this as early in the server's startup as possible
from typing import Optional
from pydantic import BaseModel
from dbt_server import tracer  # noqa

from dbt_server import models
from dbt_server.database import engine
from dbt_server.flags import WORKSPACE_ID
from dbt_server.services import dbt_service, filesystem_service
from dbt_server.views import app
from dbt_server.logging import DBT_SERVER_LOGGER as logger, configure_uvicorn_access_log
from dbt_server.state import LAST_PARSED
from dbt_server.exceptions import StateNotFoundException
from sqlalchemy.exc import OperationalError

# The default checkfirst=True should handle this, however we still
# see a table exists error from time to time
try:
    models.Base.metadata.create_all(bind=engine, checkfirst=True)
except OperationalError as err:
    logger.debug(f"Handled error when creating database: {str(err)}")

dbt_service.disable_tracking()


class ConfigArgs(BaseModel):
    target: Optional[str] = None
    profile: Optional[str] = None


def startup_cache_initialize():
    """
    Initialize the manifest cache at startup. The cache will only be populated if there is
    a latest-state-id.txt file or latest-project-path.txt file pointing to a state or project folder
    with a pre-compiled manifest. If any step fails (the latest-state-id.txt file is missing,
    there's no compiled manifest, or it can't be deserialized) then continue without caching.
    """

    # If an exception is raised in this method, the dbt-server will fail to start up.
    # Be careful here :)
    latest_state_id = filesystem_service.get_latest_state_id(None)
    latest_project_path = filesystem_service.get_latest_project_path()
    root_path = filesystem_service.get_root_path(latest_state_id, latest_project_path)

    if root_path is None:
        logger.info(
            "[STARTUP] No latest state or project found - not loading manifest into cache"
        )
        return

    manifest_path = filesystem_service.get_path(root_path, "manifest.msgpack")
    logger.info(f"[STARTUP] Loading manifest from file system (path={root_path})")

    try:
        manifest = dbt_service.deserialize_manifest(manifest_path)
    except (TypeError, ValueError) as e:
        logger.error(f"[STARTUP] Could not deserialize manifest: {str(e)}")
        return
    except (StateNotFoundException):
        logger.error(
            f"[STARTUP] Specified root path not found - not loading manifest (path={root_path})"
        )
        return

    manifest_size = filesystem_service.get_size(manifest_path)

    LAST_PARSED.set_last_parsed_manifest(
        latest_state_id, latest_project_path, root_path, manifest, manifest_size
    )

    logger.info(f"[STARTUP] Cached manifest in memory (path={root_path})")


@tracer.wrap
@app.on_event("startup")
async def startup_event():
    # This method is `async` intentionally to block serving until startup is complete
    configure_uvicorn_access_log()
    # Only run this for semantic layer environment
    if WORKSPACE_ID.get():
        startup_cache_initialize()
