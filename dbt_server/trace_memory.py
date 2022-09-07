
import tracemalloc
from dbt_server.logging import GLOBAL_LOGGER as logger


SNAPSHOT = None

class Snapshot:
    snapshot = None

    def __init__(self) -> None:
        if self.snapshot is None:
            self.snapshot = tracemalloc.take_snapshot()

    def compare_and_update_snap(self):
        new_snap = tracemalloc.take_snapshot()
        top_stats = new_snap.compare_to(self.snapshot, 'lineno')
        self.snapshot = new_snap
        logger.debug("[ Top 10 differences ]")
        for stat in top_stats[:10]:
            logger.debug(stat)
