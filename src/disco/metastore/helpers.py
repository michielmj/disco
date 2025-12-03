import threading
import uuid
from typing import Any, Callable, Dict, Optional

from kazoo.client import KazooClient, KazooState, KazooRetry
from kazoo.recipe.watchers import DataWatch, ChildrenWatch

from tools.mp_logging import getLogger
from disco.config import ZookeeperSettings

logger = getLogger(__name__)


def _zk_hosts_from_settings(settings: ZookeeperSettings) -> str:
    """
    Build the full Zookeeper connection string with optional chroot.
    Example: "host1:2181,host2:2181/disco"
    """
    if settings.chroot:
        return f"{settings.hosts}{settings.chroot}"
    return settings.hosts


def create_zk_client(settings: ZookeeperSettings) -> KazooClient:
    """
    Create a KazooClient configured from ZookeeperSettings.

    - hosts + chroot
    - session/connection timeouts
    - retry policy
    - optional auth
    - optional TLS (if supported by your Kazoo version)
    """
    hosts = _zk_hosts_from_settings(settings)

    conn_retry = KazooRetry(
        max_tries=settings.max_retries,
        delay=settings.retry_delay_s,
    )
    cmd_retry = KazooRetry(
        max_tries=settings.max_retries,
        delay=settings.retry_delay_s,
    )

    client_kwargs: dict[str, Any] = dict(
        hosts=hosts,
        timeout=settings.session_timeout_s,
        connection_retry=conn_retry,
        command_retry=cmd_retry,
        connection_timeout=settings.connection_timeout_s,
    )

    # TLS support (depending on Kazoo version)
    if settings.use_tls:
        client_kwargs["use_ssl"] = True
        # For finer-grained SSL config (CA/client certs), you may need to
        # set SSL context in your environment or pass extra options here.

    zk = KazooClient(**client_kwargs)

    if settings.auth_scheme and settings.auth_credentials:
        zk.add_auth(settings.auth_scheme, settings.auth_credentials)

    return zk


class ZkConnectionManager:
    """
    Owns a single KazooClient instance, manages its lifecycle, and makes sure
    data watches are re-installed after a session loss.

    Typical usage:
        mgr = ZkConnectionManager(settings)
        mgr.start()
        metastore = ZkMetastore(connection=mgr, group="foo")
    """

    def __init__(self, settings: ZookeeperSettings) -> None:
        self._settings = settings
        self._client: Optional[KazooClient] = None

        # watch_id -> (path, callback)
        # callback: Callable[[Optional[bytes], str], bool]
        self._watched: Dict[uuid.UUID, tuple[str, Callable[[Optional[bytes], str], bool]]] = {}

        # watch_id -> (path, callback)
        # callback: Callable[[Optional[list[str]], str], bool]
        self._children_watched: Dict[uuid.UUID, tuple[str, Callable[[Optional[list[str]], str], bool]]] = {}

        self._lock = threading.RLock()
        self._session_lost = False
        self._stopped = False

    @property
    def client(self) -> KazooClient:
        if self._client is None:
            raise RuntimeError("ZkConnectionManager not started.")
        return self._client

    @property
    def settings(self) -> ZookeeperSettings:
        return self._settings

    # --- lifecycle ---------------------------------------------------------

    def start(self) -> None:
        if self._client is not None:
            return  # idempotent

        client = create_zk_client(self._settings)
        client.add_listener(self._on_state_change)
        client.start()

        with self._lock:
            self._client = client
            self._session_lost = False
            self._stopped = False

    def stop(self) -> None:
        with self._lock:
            self._stopped = True
            client = self._client

        if client is not None:
            client.stop()
            client.close()

    @property
    def stopped(self) -> bool:
        return self._stopped

    # --- connection state handling ----------------------------------------

    def _on_state_change(self, state: KazooState) -> None:
        if state == KazooState.LOST:
            # Session has expired; ephemeral nodes and watches are gone.
            with self._lock:
                self._session_lost = True
            logger.warning("ZooKeeper session LOST; will reinitialize watches on reconnect.")

        elif state == KazooState.CONNECTED:
            # CONNECTED can be initial or after a loss; only act if we know we lost.
            with self._lock:
                if self._session_lost and not self._stopped:
                    logger.info("ZooKeeper reconnected after LOST; reinstalling data watches.")
                    self._session_lost = False
                    self._reinstall_watches()

        # SUSPENDED is usually a transient I/O issue; Kazoo will handle reconnect.
        # You may log/track it if you want, but no special logic is strictly required.

    # --- data watch registration / recovery --------------------------------

    def watch_data(self, path: str, callback: Callable[[Optional[bytes], str], bool]) -> uuid.UUID:
        """
        Register a data watch on `path`.

        callback receives (data_bytes_or_None, path) and should return:
        - True  to keep watching
        - False to stop watching (and remove from cache).
        """
        with self._lock:
            watch_id = uuid.uuid4()
            self._watched[watch_id] = (path, callback)
            self._install_data_watch(watch_id, path, callback)
            return watch_id

    def _install_data_watch(
        self,
        watch_id: uuid.UUID,
        path: str,
        callback: Callable[[Optional[bytes], str], bool],
    ) -> None:
        """
        Internal helper to attach a Kazoo DataWatch for a given cached callback.
        """

        def _wrapped(data: Optional[bytes], stat, event):
            keep = callback(data, path)
            if not keep:
                # Remove from cache if callback decides to stop
                with self._lock:
                    self._watched.pop(watch_id, None)
            return keep

        DataWatch(self.client, path, func=_wrapped)

    def watch_children(
        self,
        path: str,
        callback: Callable[[Optional[list[str]], str], bool],
    ) -> uuid.UUID:
        """
        Register a children watch on `path`.

        callback receives (children_or_None, path) and should return:
        - True  to keep watching
        - False to stop watching (and remove from cache).
        """
        with self._lock:
            watch_id = uuid.uuid4()
            self._children_watched[watch_id] = (path, callback)
            self._install_children_watch(watch_id, path, callback)
            return watch_id

    def _install_children_watch(
        self,
        watch_id: uuid.UUID,
        path: str,
        callback: Callable[[Optional[list[str]], str], bool],
    ) -> None:
        """
        Internal helper to attach a Kazoo ChildrenWatch for a given cached callback.
        """

        def _wrapped(children):
            keep = callback(children, path)
            if not keep:
                with self._lock:
                    self._children_watched.pop(watch_id, None)
            return keep

        ChildrenWatch(self.client, path, func=_wrapped)

    def _reinstall_watches(self) -> None:
        """
        Reinstall all cached data watches after a LOST -> CONNECTED transition.
        """

        data_items = list(self._watched.items())
        children_items = list(self._children_watched.items())

        for watch_id, (path, callback) in data_items:
            logger.debug("Reinstalling data watch %s on path %s", watch_id, path)
            self._install_data_watch(watch_id, path, callback)

        for watch_id, (path, callback) in children_items:
            logger.debug("Reinstalling children watch %s on path %s", watch_id, path)
            self._install_children_watch(watch_id, path, callback)
