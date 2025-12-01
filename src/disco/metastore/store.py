from __future__ import annotations

import pickle
import uuid
from time import sleep, time
from typing import Any, Callable, Dict, Optional, TypeAlias, cast

from kazoo.client import KazooClient

from tools.mp_logging import getLogger

from .helpers import ZkConnectionManager
from .structure import BASE_STRUCTURE


ExpandType: TypeAlias = Dict[str, "ExpandType"] | list | None

QUEUE_POLLING_S = 0.05   # polling for Kazoo queues

logger = getLogger(__name__)


class Metastore:
    """
    High-level metadata store on top of ZooKeeper.

    - Uses ZkConnectionManager (one client per process).
    - Optionally namespaces paths under '/<group>' inside ZooKeeper's chroot.
    - Recovers watchers after session loss via ZkConnectionManager.
    - Supports pluggable serialization (default = pickle).
    """

    def __init__(
        self,
        connection: "ZkConnectionManager",
        group: Optional[str] = None,
        packb: Callable[[Any], bytes] = pickle.dumps,
        unpackb: Callable[[bytes], Any] = pickle.loads,
    ) -> None:
        self._connection = connection
        self._group = group

        self._packb = packb
        self._unpackb = unpackb

        self._queue_polling_interval = QUEUE_POLLING_S

        # Ensure base structure under chroot + optional /<group>
        for path in BASE_STRUCTURE:
            full = self._full_path(path)
            self.client.ensure_path(full)

    # ----------------------------------------------------------------------
    # Internal helpers
    # ----------------------------------------------------------------------

    @property
    def group(self) -> Optional[str]:
        return self._group

    @property
    def client(self) -> KazooClient:
        return self._connection.client

    def _full_path(self, path: str) -> str:
        """
        Build the ZooKeeper path inside the chroot configured on the client.
        Only adds '/<group>' if group is configured.
        """
        rel = (path or "").lstrip("/")

        if self._group:
            return f"/{self._group}/{rel}" if rel else f"/{self._group}"
        else:
            return f"/{rel}" if rel else "/"

    # ----------------------------------------------------------------------
    # Watcher registration (recovered after session loss)
    # ----------------------------------------------------------------------

    def watch_with_callback(
        self,
        path: str,
        callback: Callable[[Any, str], bool],
    ) -> uuid.UUID:
        """
        Register a watch on `path`. Callback receives (decoded_value, full_path).
        If callback returns False, the watch is removed.
        """
        full_path = self._full_path(path)

        def _wrapped(raw: Optional[bytes], p: str) -> bool:
            if raw is None:
                # Node deleted → stop watching.
                return False
            value = self._unpackb(raw)
            return callback(value, p)

        return self._connection.watch_data(full_path, _wrapped)

    # ----------------------------------------------------------------------
    # Key-value operations
    # ----------------------------------------------------------------------

    def get_key(self, path: str) -> Any:
        full_path = self._full_path(path)

        if not self.client.exists(full_path):
            return None

        data, _stat = self.client.get(full_path)
        if not data:
            return None
        return self._unpackb(data)

    def __getitem__(self, item: str) -> Any:
        return self.get_key(item)

    def update_key(self, path: str, value: Any) -> None:
        full_path = self._full_path(path)
        data = self._packb(value)

        if self.client.exists(full_path):
            self.client.set(full_path, data)
        else:
            self.client.create(full_path, data, makepath=True)

    def drop_key(self, path: str) -> bool:
        full_path = self._full_path(path)
        if self.client.exists(full_path):
            self.client.delete(full_path, recursive=True)
            return True
        return False

    def __contains__(self, item: str) -> bool:
        return bool(self.client.exists(self._full_path(item)))

    def list_members(self, path: str) -> list[str]:
        return cast(list[str], self.client.get_children(self._full_path(path)))

    # ----------------------------------------------------------------------
    # Hierarchical get/update
    # ----------------------------------------------------------------------

    def get_keys(
            self,
            path: str,
            expand: dict[str, "ExpandType"] | None = None,
    ) -> dict[str, Any] | None:
        """
        Returns all keys and values found at the logical path `path`.

        `expand` is a nested dict with the same semantics as in update_keys.
        """
        full_path = self._full_path(path)

        if not self.client.exists(full_path):
            return None

        expand = expand or {}
        data: dict[str, Any] = {}

        base = path.rstrip("/") if path else ""

        for member in self.client.get_children(full_path):
            member_rel = f"{base}/{member}" if base else member
            member_full = self._full_path(member_rel)

            if isinstance(expand, dict) and member in expand:
                cfg = expand[member]  # may be dict or None
                children = self.client.get_children(member_full)
                data[member] = {}

                if isinstance(cfg, dict):
                    # nested expansion allowed: recurse
                    for child in children:
                        child_rel = f"{member_rel}/{child}"
                        data[member][child] = self.get_keys(child_rel, expand=cfg)
                else:
                    # cfg is None -> expand one level, children are leaves
                    for child in children:
                        child_rel = f"{member_rel}/{child}"
                        data[member][child] = self.get_key(child_rel)
            else:
                data[member] = self.get_key(member_rel)

        return data

    def update_keys(
        self,
        path: str,
        members: dict[str, Any],
        expand: dict[str, "ExpandType"] | None = None,
        drop: bool = False,
    ) -> None:
        """
        Stores all keys and values in `members` under logical path `path`.

        `expand` is a nested dict with the following semantics:

          - If expand[key] is a dict: expand into children and pass that dict down.
          - If expand[key] is None: expand one level, children are leaves.

        Example (relative to `path`):

          expand = {"replications": {"assignments": None}}
          members = {"replications": {"r1": {"assignments": {"a": 1}}}}
          -> "/replications/r1/assignments/a" = 1

          expand = {"replications": None}
          members = {"replications": {"assignments": {"a": 1}}}
          -> "/replications/assignments" = {"a": 1}
        """
        full_path = self._full_path(path)

        if drop and self.client.exists(full_path):
            self.client.delete(full_path, recursive=True)

        self.client.ensure_path(full_path)

        expand = expand or {}
        base = path.rstrip("/") if path else ""

        for key, value in members.items():
            key_rel = f"{base}/{key}" if base else key

            if isinstance(expand, dict) and key in expand and isinstance(value, dict):
                cfg = expand[key]  # may be dict or None

                # Two cases:
                # - cfg is dict  -> nested expansion allowed
                # - cfg is None  -> expand one level, children are leaves
                for child_key, child_value in value.items():
                    child_rel = f"{key_rel}/{child_key}"

                    if isinstance(cfg, dict) and isinstance(child_value, dict):
                        # further nested expansion
                        self.update_keys(child_rel, child_value, expand=cfg)
                    else:
                        # either cfg is None (one level only) or child_value is scalar
                        self.update_key(child_rel, child_value)
            else:
                self.update_key(key_rel, value)

    # ----------------------------------------------------------------------
    # Queue operations
    # ----------------------------------------------------------------------

    def enqueue(self, path: str, value: Any) -> None:
        full_path = self._full_path(path)
        queue = self.client.Queue(full_path)
        queue.put(self._packb(value))

    def dequeue(self, path: str, timeout: Optional[float] = None) -> Any:
        full_path = self._full_path(path)
        queue = self.client.Queue(full_path)

        deadline = float("inf") if timeout is None else time() + timeout

        item = queue.get()
        while item is None and time() < deadline:
            sleep(self._queue_polling_interval)
            item = queue.get()

        if item is None:
            return None
        return self._unpackb(item)
