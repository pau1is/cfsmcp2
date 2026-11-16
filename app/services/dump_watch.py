"""Poll path-mode dumps and queue ``parse_entity`` when tracked files change.

inotify is not used: Docker Desktop bind-mounts from Windows often drop
filesystem events. A file that was never in ``dump_file_state`` is not
detected until some already tracked file changes (a 1C dump updates
``ConfigDumpInfo.xml``).
"""

from __future__ import annotations

import logging
import threading
from pathlib import Path

from app.core.config import settings
from app.core.database import connect
from app.repositories import dump_files as dump_file_repo
from app.repositories import entities as ent_repo
from app.services.dump_file_index import tracked_files_unchanged
from app.services import runtime_settings

log = logging.getLogger("cfsmcp2.dump_watch")

_stop = threading.Event()
_thread: threading.Thread | None = None
_STARTUP_DELAY_SEC = 5


def start() -> None:
    global _thread
    if _thread is not None and _thread.is_alive():
        return
    _stop.clear()
    _thread = threading.Thread(target=_loop, name="dump-watch", daemon=True)
    _thread.start()
    log.info(
        "dump watch started interval=%ss (0 disables)",
        runtime_settings.get_dump_watch_interval_sec(),
    )


def shutdown() -> None:
    _stop.set()


def _loop() -> None:
    if _stop.wait(_STARTUP_DELAY_SEC):
        return
    while not _stop.is_set():
        interval = runtime_settings.get_dump_watch_interval_sec()
        if interval <= 0:
            if _stop.wait(_STARTUP_DELAY_SEC):
                return
            continue
        try:
            queued = scan_once()
            if queued:
                log.info("dump watch queued parse entities=%s", queued)
        except Exception:
            log.exception("dump watch scan failed")
        if _stop.wait(interval):
            return


def scan_once() -> list[int]:
    """Queue incremental parse for ready path-dumps whose stamps drifted."""
    from app.services.dump_zip import resolve_dump_root
    from app.services.jobs import submit
    from app.services.pipeline import parse_entity

    conn = connect(settings.db_path)
    try:
        entities = ent_repo.list_entities(conn)
        snapshots: list[tuple[dict, dict[str, tuple[int, int]]]] = []
        for entity in entities:
            if not _watchable(entity):
                continue
            stamps = dump_file_repo.load_stamps(conn, int(entity["id"]))
            if not stamps:
                continue
            snapshots.append((entity, stamps))
    finally:
        conn.close()

    queued: list[int] = []
    for entity, stamps in snapshots:
        root = _dump_root(entity, resolve_dump_root)
        if root is None:
            continue
        if tracked_files_unchanged(root, stamps):
            continue
        entity_id = int(entity["id"])
        submit(parse_entity, entity_id)
        queued.append(entity_id)
        log.info(
            "dump watch changed entity=%s name=%s files=%s",
            entity_id,
            entity.get("name") or "",
            len(stamps),
        )
    return queued


def _watchable(entity: dict) -> bool:
    if not entity.get("enabled"):
        return False
    if (entity.get("source_location") or "") != "path":
        return False
    if (entity.get("source_mode") or "") != "dump":
        return False
    return (entity.get("status") or "") == "ready"


def _dump_root(entity: dict, resolve_dump_root) -> Path | None:
    raw = str(entity.get("dumps_dir") or "").strip()
    if not raw:
        return None
    root = resolve_dump_root(Path(raw))
    if not (root / "Configuration.xml").is_file():
        return None
    return root
