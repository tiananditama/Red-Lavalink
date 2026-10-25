from __future__ import annotations

import asyncio
import secrets
import string
import typing
from collections import namedtuple
from typing import KeysView, List, Optional, ValuesView

import aiohttp
from discord.backoff import ExponentialBackoff
from discord.ext.commands import Bot

from . import log, ws_ll_log, ws_rll_log, __version__
from .enums import LavalinkEvents, LavalinkIncomingOp, LavalinkOutgoingOp, NodeState, PlayerState
from .player import Player
from .rest_api import Track
from .utils import VoiceChannel, is_loop_closed
from .errors import AbortingNodeConnection, NodeNotReady, NodeNotFound, PlayerNotFound

__all__ = [
    "Stats",
    "Node",
    "NodeStats",
    "get_node",
    "get_nodes_stats",
    "get_all_nodes",
]

_nodes: List[Node] = []

PositionTime = namedtuple("PositionTime", "position time connected")
MemoryInfo = namedtuple("MemoryInfo", "reservable used free allocated")
CPUInfo = namedtuple("CPUInfo", "cores systemLoad lavalinkLoad")

_V4_TRACK_END_REASONS = {
    "finished": "FINISHED",
    "loadFailed": "LOAD_FAILED",
    "stopped": "STOPPED",
    "replaced": "REPLACED",
    "cleanup": "CLEANUP",
}


# Originally Added in: https://github.com/PythonistaGuild/Wavelink/pull/66
class _Key:
    def __init__(self, length: int = 32):
        self.length: int = length
        self.persistent: str = ""
        self.__repr__()

    def __repr__(self):
        """Generate a new key, return it and make it persistent"""
        alphabet = string.ascii_letters + string.digits + "#$%&()*+,-./:;<=>?@[]^_~!"
        key = "".join(secrets.choice(alphabet) for _ in range(self.length))
        self.persistent = key
        return key

    def __str__(self):
        """Return the persistent key."""
        # Ensure output is not a non-string
        # Since input could be Any object.
        if not self.persistent:
            return self.__repr__()
        return str(self.persistent)


class Stats:
    def __init__(self, memory, players, active_players, cpu, uptime):
        self.memory = MemoryInfo(**memory)
        self.players = players
        self.active_players = active_players
        self.cpu_info = CPUInfo(**cpu)
        self.uptime = uptime


# Node stats related class below and how it is called is originally from:
# https://github.com/PythonistaGuild/Wavelink/blob/abba49e9806af3c50886f82054ea603129ad08b9/wavelink/stats.py#L41
# https://github.com/PythonistaGuild/Wavelink/blob/abba49e9806af3c50886f82054ea603129ad08b9/wavelink/websocket.py#L132
class NodeStats:
    def __init__(self, data: dict):
        self.uptime = data["uptime"]

        self.players = data["players"]
        self.playing_players = data["playingPlayers"]

        memory = data["memory"]
        self.memory_free = memory["free"]
        self.memory_used = memory["used"]
        self.memory_allocated = memory["allocated"]
        self.memory_reservable = memory["reservable"]

        cpu = data["cpu"]
        self.cpu_cores = cpu["cores"]
        self.system_load = cpu["systemLoad"]
        self.lavalink_load = cpu["lavalinkLoad"]

        frame_stats = data.get("frameStats") or {}
        self.frames_sent = frame_stats.get("sent", -1)
        self.frames_nulled = frame_stats.get("nulled", -1)
        self.frames_deficit = frame_stats.get("deficit", -1)

    def __repr__(self):
        return (
            "<NodeStats: "
            f"uptime={self.uptime}, "
            f"players={self.players}, "
            f"playing_players={self.playing_players}, "
            f"memory_free={self.memory_free}, memory_used={self.memory_used}, "
            f"cpu_cores={self.cpu_cores}, system_load={self.system_load}, "
            f"lavalink_load={self.lavalink_load}>"
        )


class Node:
    _is_shutdown: bool = False

    def __init__(
        self,
        *,
        event_handler: typing.Callable,
        host: str,
        password: str,
        user_id: int,
        num_shards: int,
        port: Optional[int] = None,
        resume_key: Optional[str] = None,
        resume_timeout: float = 60,
        bot: Bot = None,
        secured: bool = False,
    ):
        """
        Represents a Lavalink node.

        Parameters
        ----------
        event_handler
            Function to dispatch events to.
        host : str
            Lavalink player host.
        password : str
            Password for the Lavalink player.
        port : Optional[int]
            Port of the Lavalink player event websocket.
        user_id : int
            User ID of the bot.
        num_shards : int
            Number of shards to which the bot is currently connected.
        resume_key : Optional[str]
            A resume key used for resuming a session upon re-establishing a WebSocket connection to Lavalink.
        resume_timeout : float
            How long the node should wait for a connection while disconnected before clearing all players.
        bot: AutoShardedBot
            The Bot object that connects to discord.
        """
        self.bot = bot
        self.event_handler = event_handler
        self.host = host
        self.secured = secured
        if port is None:
            if self.secured:
                self.port = 443
            else:
                self.port = 80
        else:
            self.port = port
        self.password = password
        self._resume_key = resume_key
        if self._resume_key is None:
            self._resume_key = self._gen_key()
        self._resume_timeout = resume_timeout
        self._resuming_configured = False
        self._major_version: Optional[int] = None
        self._session_id: Optional[str] = None
        self._session_id_event = asyncio.Event()
        self._filters: dict = {}
        self.num_shards = num_shards
        self.user_id = user_id

        self._ready_event = asyncio.Event()

        self._ws = None
        self._listener_task = None
        self.session = aiohttp.ClientSession()
        self.reconnect_task = None
        self.try_connect_task = None

        self._queue: List = []
        self._players_dict = {}

        self.state = NodeState.CONNECTING
        self._state_handlers: List = []
        self._retries = 0

        self.stats = None

        if self not in _nodes:
            _nodes.append(self)

        self._closers = (
            aiohttp.WSMsgType.CLOSE,
            aiohttp.WSMsgType.CLOSING,
            aiohttp.WSMsgType.CLOSED,
        )

        self.register_state_handler(self.node_state_handler)

    def __repr__(self):
        return (
            "<Node: "
            f"state={self.state.name}, "
            f"host={self.host}, "
            f"port={self.port}, "
            f"password={'*' * len(self.password)}, resume_key={self._resume_key}, "
            f"shards={self.num_shards}, user={self.user_id}, stats={self.stats}>"
        )

    @property
    def headers(self) -> dict:
        return self._get_connect_headers()

    @property
    def players(self) -> ValuesView[Player]:
        return self._players_dict.values()

    @property
    def guild_ids(self) -> KeysView[int]:
        return self._players_dict.keys()

    def _gen_key(self):
        if self._resume_key is None:
            return _Key()
        else:
            # if this is a class then it will generate a persistent key
            # We should not't check the instance since
            # we would still make 1 extra call to check, which is useless.
            self._resume_key.__repr__()
            return self._resume_key

    async def connect(self, timeout: float = None, *, shutdown: bool = False):
        """
        Connects to the Lavalink player event websocket.

        Parameters
        ----------
        timeout : float
            Time after which to timeout on attempting to connect to the Lavalink websocket,
            ``None`` is considered never, but the underlying code may stop trying past a
            certain point.
        shutdown : bool
            Whether the node was told to shut down

        Raises
        ------
        asyncio.TimeoutError
            If the websocket failed to connect after the given time.
        AbortingNodeConnection
            If the connection attempt must be aborted during a reconnect attempt
        """
        self._is_shutdown = shutdown
        if self._major_version is None:
            await self._detect_major_version()
        path = "/v4/websocket" if self.protocol_version >= 4 else ""
        if self.secured:
            uri = f"wss://{self.host}:{self.port}{path}"
        else:
            uri = f"ws://{self.host}:{self.port}{path}"

        ws_ll_log.info("Lavalink WS connecting to %s with headers %s", uri, self.headers)
        if self.try_connect_task is not None:
            self.try_connect_task.cancel()
        self.try_connect_task = asyncio.create_task(self._multi_try_connect(uri))
        try:
            await asyncio.wait_for(self.try_connect_task, timeout=timeout)
        except asyncio.CancelledError:
            raise AbortingNodeConnection

    async def _configure_resume(self):
        if self._resuming_configured:
            return
        if not (self._resume_key and self._resume_timeout and self._resume_timeout > 0):
            return
        if self.protocol_version >= 4:
            try:
                await asyncio.wait_for(self._session_id_event.wait(), timeout=10)
            except asyncio.TimeoutError:
                ws_ll_log.warning(
                    "Timed out waiting for a Lavalink session ID; resuming not configured."
                )
                return
            if not self._session_id:
                return
            await self._rest_request(
                "PATCH",
                f"/v4/sessions/{self._session_id}",
                json={"resuming": True, "timeout": int(self._resume_timeout)},
            )
        else:
            await self.send(
                dict(
                    op="configureResuming",
                    key=str(self._resume_key),
                    timeout=self._resume_timeout,
                )
            )
        self._resuming_configured = True
        ws_ll_log.debug("Node Resuming has been configured.")

    async def wait_until_ready(self, *, timeout: Optional[float] = None):
        await asyncio.wait_for(self._ready_event.wait(), timeout=timeout)

    def _get_connect_headers(self) -> dict:
        # Num-Shards is not used on Lavalink jar files >= v3.4
        # but kept for compatibility to avoid NPEs on older builds
        headers = {
            "Authorization": self.password,
            "User-Id": str(self.user_id),
            "Num-Shards": str(self.num_shards),
            "Client-Name": f"Red-Lavalink/{__version__}",
        }
        if self._resume_key:
            headers["Resume-Key"] = str(self._resume_key)
        return headers

    @property
    def protocol_version(self) -> int:
        """The Lavalink protocol major version this node uses (3 or 4)."""
        return self._major_version or 3

    async def _detect_major_version(self) -> None:
        protocol = "https" if self.secured else "http"
        url = f"{protocol}://{self.host}:{self.port}/v4/info"
        try:
            async with self.session.get(
                url,
                headers={"Authorization": self.password},
                timeout=aiohttp.ClientTimeout(total=5),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    if isinstance(data, dict) and "version" in data:
                        self._major_version = 4
                        ws_ll_log.info("Detected Lavalink v4 protocol")
                        return
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as exc:
            ws_ll_log.debug("Failed to detect Lavalink protocol version: %s", exc)
        self._major_version = 3
        ws_ll_log.info("Detected Lavalink v3 protocol")

    @property
    def lavalink_major_version(self):
        if not self.ready:
            raise NodeNotReady("Node not ready!")
        return self._ws.response_headers.get("Lavalink-Major-Version")

    @property
    def ready(self) -> bool:
        """
        Whether the underlying node is ready for requests.
        """
        return self.state == NodeState.READY

    async def _multi_try_connect(self, uri):
        backoff = ExponentialBackoff()
        attempt = 1
        self._session_id = None
        self._session_id_event.clear()
        self._resuming_configured = False
        if self._listener_task is not None:
            self._listener_task.cancel()
        if self._ws is not None:
            await self._ws.close(code=4006, message=b"Reconnecting")

        while self._is_shutdown is False and (self._ws is None or self._ws.closed):
            self._retries += 1
            if self._is_shutdown is True:
                ws_ll_log.error("Lavalink node was shutdown during a connect attempt.")
                raise asyncio.CancelledError
            try:
                ws = await self.session.ws_connect(url=uri, headers=self.headers, heartbeat=60)
            except (OSError, aiohttp.ClientConnectionError):
                if attempt > 5:
                    raise asyncio.TimeoutError
                delay = backoff.delay()
                ws_ll_log.warning("Failed connect attempt %s, retrying in %s", attempt, delay)
                await asyncio.sleep(delay)
                attempt += 1
            except aiohttp.WSServerHandshakeError:
                ws_ll_log.error("Failed connect WSServerHandshakeError")
                raise asyncio.TimeoutError
            else:
                if self._is_shutdown is True:
                    ws_ll_log.error("Lavalink node was shutdown during a connect attempt.")
                    raise asyncio.CancelledError
                self.session_resumed = ws._response.headers.get("Session-Resumed", False)
                if self._ws is not None and self.session_resumed:
                    ws_ll_log.info("WEBSOCKET Resumed Session with key: %s", self._resume_key)
                self._ws = ws
                break
        if self._is_shutdown is True:
            raise asyncio.CancelledError
        ws_ll_log.info("Lavalink WS connected to %s", uri)
        ws_ll_log.debug("Creating Lavalink WS listener.")
        if self._is_shutdown is False:
            self._listener_task = asyncio.create_task(self.listener())
            asyncio.create_task(self._configure_resume())
            if self.protocol_version < 4 and self._queue:
                temp = self._queue.copy()
                self._queue.clear()
                for data in temp:
                    await self.send(data)
            self._ready_event.set()
            self.update_state(NodeState.READY)

    async def listener(self):
        """
        Listener task for receiving ops from Lavalink.
        """
        while self._is_shutdown is False:
            msg = await self._ws.receive()
            if msg.type in self._closers:
                if self._resuming_configured:
                    if self.state != NodeState.RECONNECTING:
                        if self.reconnect_task is not None:
                            self.reconnect_task.cancel()
                        ws_ll_log.info("[NODE] | NODE Resuming: %s", msg.extra)
                        self.update_state(NodeState.RECONNECTING)
                        self.reconnect_task = asyncio.create_task(
                            self._reconnect(shutdown=self._is_shutdown)
                        )
                    return
                else:
                    ws_ll_log.info("[NODE] | Listener closing: %s", msg.extra)
                    break
            elif msg.type == aiohttp.WSMsgType.TEXT:
                data = msg.json()
                try:
                    op = LavalinkIncomingOp(data.get("op"))
                except ValueError:
                    ws_ll_log.verbose("[NODE] | Received unknown op: %s", data)
                else:
                    ws_ll_log.trace("[NODE] | Received known op: %s", data)
                    asyncio.create_task(self._handle_op(op, data))
            elif msg.type == aiohttp.WSMsgType.ERROR:
                exc = self._ws.exception()
                ws_ll_log.warning(
                    "[NODE] | An exception occurred on the websocket - Attempting to reconnect"
                )
                ws_ll_log.debug("[NODE] | Exception in WebSocket!", exc_info=exc)
                break
            else:
                ws_ll_log.debug(
                    "[NODE] | WebSocket connection received unexpected message: %s:%s",
                    msg.type,
                    msg.data,
                )
        if self.state != NodeState.RECONNECTING and not self._is_shutdown:
            ws_ll_log.warning(
                "[NODE] | %s - WS %s SHUTDOWN %s.", self, not self._ws.closed, self._is_shutdown
            )
            if self.reconnect_task is not None:
                self.reconnect_task.cancel()
            self.update_state(NodeState.RECONNECTING)
            self.reconnect_task = asyncio.create_task(self._reconnect(shutdown=self._is_shutdown))

    def _normalize_v4_event(self, data: dict) -> dict:
        normalized = dict(data)
        track = normalized.get("track")
        if isinstance(track, dict):
            normalized["track"] = track.get("encoded")
        reason = normalized.get("reason")
        if normalized.get("type") == LavalinkEvents.TRACK_END.value and isinstance(reason, str):
            normalized["reason"] = _V4_TRACK_END_REASONS.get(reason, reason.upper())
        exception = normalized.get("exception")
        if isinstance(exception, dict):
            exc = dict(exception)
            severity = exc.get("severity")
            if isinstance(severity, str):
                exc["severity"] = severity.upper()
            exc["message"] = exc.get("message") or ""
            exc["cause"] = exc.get("cause") or ""
            normalized["exception"] = exc
        return normalized

    async def _handle_op(self, op: LavalinkIncomingOp, data):
        if op == LavalinkIncomingOp.READY:
            self._session_id = data.get("sessionId")
            if self._session_id:
                self._session_id_event.set()
            ws_ll_log.debug(
                "Lavalink session ready: %s (resumed=%s)",
                self._session_id,
                data.get("resumed"),
            )
            if self.protocol_version >= 4:
                asyncio.create_task(self._flush_queue())
            return
        if op == LavalinkIncomingOp.EVENT:
            if self.protocol_version >= 4:
                data = self._normalize_v4_event(data)
            try:
                event = LavalinkEvents(data.get("type"))
            except ValueError:
                ws_ll_log.verbose("Unknown event type: %s", data)
            else:
                self.event_handler(op, event, data)
        elif op == LavalinkIncomingOp.PLAYER_UPDATE:
            state = data.get("state", {})
            position = PositionTime(
                position=state.get("position", 0),
                time=state.get("time", 0),
                connected=state.get("connected", False),
            )
            self.event_handler(op, position, data)
        elif op == LavalinkIncomingOp.STATS:
            stats = Stats(
                memory=data.get("memory"),
                players=data.get("players"),
                active_players=data.get("playingPlayers"),
                cpu=data.get("cpu"),
                uptime=data.get("uptime"),
            )
            self.stats = NodeStats(data)
            self.event_handler(op, stats, data)
        else:
            ws_ll_log.verbose("Unknown op type: %r", data)

    async def _reconnect(self, *, shutdown: bool = False):
        self._ready_event.clear()
        self._session_id = None
        self._session_id_event.clear()

        if self._is_shutdown is True or shutdown:
            ws_ll_log.info("[NODE] | Shutting down Lavalink WS.")
            return
        if self.state != NodeState.CONNECTING:
            self.update_state(NodeState.RECONNECTING)
        if self.state != NodeState.RECONNECTING:
            return
        backoff = ExponentialBackoff(base=1)
        attempt = 1
        while self.state == NodeState.RECONNECTING:
            attempt += 1
            if attempt > 10:
                ws_ll_log.info("[NODE] | Failed reconnection attempt too many times, aborting ...")
                asyncio.create_task(self.disconnect())
                return
            try:
                await self.connect(shutdown=shutdown)
            except AbortingNodeConnection:
                return
            except asyncio.TimeoutError:
                delay = backoff.delay()
                ws_ll_log.warning(
                    "[NODE] | Lavalink WS reconnect attempt %s, retrying in %s",
                    attempt,
                    delay,
                )
                await asyncio.sleep(delay)
            else:
                ws_ll_log.info("[NODE] | Reconnect successful.")
                self.dispatch_reconnect()
                self._retries = 0

    def dispatch_reconnect(self):
        for guild_id in self.guild_ids:
            self.event_handler(
                LavalinkIncomingOp.EVENT,
                LavalinkEvents.WEBSOCKET_CLOSED,
                {
                    "guildId": guild_id,
                    "code": 42069,
                    "reason": "Lavalink WS reconnected",
                    "byRemote": True,
                    "retries": self._retries,
                },
            )

    def update_state(self, next_state: NodeState):
        if next_state == self.state:
            return

        ws_ll_log.verbose("Changing node state: %s -> %s", self.state.name, next_state.name)
        old_state = self.state
        self.state = next_state
        if is_loop_closed():
            ws_ll_log.debug("Event loop closed, not notifying state handlers.")
            return
        for handler in self._state_handlers:
            asyncio.create_task(handler(next_state, old_state))

    def register_state_handler(self, func):
        if not asyncio.iscoroutinefunction(func):
            raise ValueError("Argument must be a coroutine object.")

        if func not in self._state_handlers:
            self._state_handlers.append(func)

    def unregister_state_handler(self, func):
        self._state_handlers.remove(func)

    async def create_player(self, channel: VoiceChannel, *, self_deaf: bool = False) -> Player:
        """
        Connects to a discord voice channel.

        This function is safe to repeatedly call as it will return an existing
        player if there is one.

        Parameters
        ----------
        channel: VoiceChannel
        self_deaf: bool

        Returns
        -------
        Player
            The created Player object.
        """
        if self._already_in_guild(channel):
            player = self.get_player(channel.guild.id)
            await player.move_to(channel, self_deaf=self_deaf)
        else:
            player: Player = await channel.connect(cls=Player, self_deaf=self_deaf)  # type: ignore
        return player

    def _already_in_guild(self, channel: VoiceChannel) -> bool:
        return channel.guild.id in self._players_dict

    def get_player(self, guild_id: int) -> Player:
        """
        Gets a Player object from a guild ID.

        Parameters
        ----------
        guild_id : int
            Discord guild ID.

        Returns
        -------
        Player

        Raises
        ------
        KeyError
            If that guild does not have a Player, e.g. is not connected to any
            voice channel.
        """
        if guild_id in self._players_dict:
            return self._players_dict[guild_id]
        raise PlayerNotFound("No such player for that guild.")

    async def node_state_handler(self, next_state: NodeState, old_state: NodeState):
        ws_rll_log.debug("Received node state update: %s -> %s", old_state.name, next_state.name)
        if next_state == NodeState.READY:
            await self.update_player_states(PlayerState.READY)
        elif next_state == NodeState.DISCONNECTING:
            await self.update_player_states(PlayerState.DISCONNECTING)
        elif next_state in (NodeState.CONNECTING, NodeState.RECONNECTING):
            await self.update_player_states(PlayerState.NODE_BUSY)

    async def update_player_states(self, state: PlayerState):
        for player in self.players:
            await player.update_state(state)

    async def refresh_player_state(self, player: Player):
        if self.ready:
            await player.update_state(PlayerState.READY)
        elif self.state == NodeState.DISCONNECTING:
            await player.update_state(PlayerState.DISCONNECTING)
        else:
            await player.update_state(PlayerState.NODE_BUSY)

    def remove_player(self, player: Player):
        if player.state != PlayerState.DISCONNECTING:
            log.error(
                "Attempting to remove a player (%r) from player list with state: %s",
                player,
                player.state.name,
            )
            return
        guild_id = player.channel.guild.id
        if guild_id in self._players_dict:
            del self._players_dict[guild_id]

    async def disconnect(self):
        """
        Shuts down and disconnects the websocket.
        """
        global _nodes
        self._is_shutdown = True
        self._ready_event.clear()
        self._queue.clear()
        if (
            self.try_connect_task is not None
            and not self.try_connect_task.cancelled()
            and not is_loop_closed()
        ):
            self.try_connect_task.cancel()
        if (
            self.reconnect_task is not None
            and not self.reconnect_task.cancelled()
            and not is_loop_closed()
        ):
            self.reconnect_task.cancel()

        self.update_state(NodeState.DISCONNECTING)

        if self._resuming_configured:
            if self.protocol_version >= 4 and self._session_id:
                await self._rest_request(
                    "PATCH",
                    f"/v4/sessions/{self._session_id}",
                    json={"resuming": False, "timeout": 0},
                )
            elif not (self._ws is None or self._ws.closed):
                await self.send(dict(op="configureResuming", key=None))
        self._session_id = None
        self._resuming_configured = False

        for p in tuple(self.players):
            await p.disconnect(force=True)
        log.debug("Disconnected all players.")

        if self._ws is not None and not self._ws.closed:
            await self._ws.close()

        if (
            self._listener_task is not None
            and not self._listener_task.cancelled()
            and not is_loop_closed()
        ):
            self._listener_task.cancel()

        await self.session.close()

        self._state_handlers = []
        if len(_nodes) == 1:
            _nodes = []
        elif len(_nodes) > 1:
            _nodes.remove(self)
        ws_ll_log.info("Shutdown Lavalink WS.")

    async def send(self, data):
        if self.protocol_version >= 4:
            await self._send_v4(data)
            return
        if self._ws is None or self._ws.closed:
            self._queue.append(data)
        else:
            ws_ll_log.trace("Sending data to Lavalink node: %s", data)
            await self._ws.send_json(data)

    def _player_path(self, guild_id) -> str:
        return f"/v4/sessions/{self._session_id}/players/{guild_id}"

    async def _rest_request(self, method: str, path: str, *, params=None, json=None):
        protocol = "https" if self.secured else "http"
        url = f"{protocol}://{self.host}:{self.port}{path}"
        try:
            async with self.session.request(
                method,
                url,
                headers={"Authorization": self.password},
                params=params,
                json=json,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status == 204:
                    return None
                if resp.status >= 400:
                    body = await resp.text()
                    ws_ll_log.error(
                        "Lavalink REST request failed: %s %s (%s): %s",
                        method,
                        path,
                        resp.status,
                        body,
                    )
                    return None
                return await resp.json(content_type=None)
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            ws_ll_log.error("Lavalink REST request failed: %s %s: %s", method, path, exc)
            return None

    async def _send_v4(self, data):
        op = data.get("op")
        if op == "configureResuming":
            if not self._session_id:
                return
            resuming = data.get("key") is not None
            timeout = int(data.get("timeout") or 0) if resuming else 0
            await self._rest_request(
                "PATCH",
                f"/v4/sessions/{self._session_id}",
                json={"resuming": resuming, "timeout": timeout},
            )
            return

        if self._session_id is None:
            self._queue.append(data)
            return

        guild_id = data.get("guildId")
        path = self._player_path(guild_id)

        if op == LavalinkOutgoingOp.PLAY.value:
            payload = {
                "track": {"encoded": data.get("track")},
                "position": int(data.get("startTime") or 0),
                "paused": bool(data.get("pause", False)),
            }
            params = {"noReplace": "true"} if data.get("noReplace") else None
            await self._rest_request("PATCH", path, params=params, json=payload)
        elif op == LavalinkOutgoingOp.STOP.value:
            await self._rest_request("PATCH", path, json={"track": {"encoded": None}})
        elif op == LavalinkOutgoingOp.PAUSE.value:
            await self._rest_request("PATCH", path, json={"paused": bool(data.get("pause"))})
        elif op == LavalinkOutgoingOp.SEEK.value:
            await self._rest_request(
                "PATCH", path, json={"position": int(data.get("position") or 0)}
            )
        elif op == LavalinkOutgoingOp.VOLUME.value:
            await self._rest_request("PATCH", path, json={"volume": int(data.get("volume") or 0)})
        elif op == "equalizer":
            guild_key = str(guild_id)
            filters = dict(self._filters.get(guild_key, {}))
            filters["equalizer"] = data.get("bands") or []
            self._filters[guild_key] = filters
            await self._rest_request("PATCH", path, json={"filters": filters})
        elif op == LavalinkOutgoingOp.DESTROY.value:
            await self._rest_request("DELETE", path)
        elif op == LavalinkOutgoingOp.VOICE_UPDATE.value:
            event = data.get("event") or {}
            voice = {
                "token": event.get("token"),
                "endpoint": event.get("endpoint"),
                "sessionId": data.get("sessionId"),
            }
            if not all(voice.values()):
                ws_ll_log.warning("Refusing to send incomplete voice update: %s", voice)
                return
            await self._rest_request("PATCH", path, json={"voice": voice})
        else:
            ws_ll_log.warning("Unsupported Lavalink v4 op: %s", data)

    async def _flush_queue(self):
        if self.protocol_version < 4 or self._session_id is None:
            return
        queue = self._queue
        self._queue = []
        for data in queue:
            await self._send_v4(data)

    async def send_lavalink_voice_update(self, guild_id, session_id, channel_id, event):
        await self.send(
            {
                "op": LavalinkOutgoingOp.VOICE_UPDATE.value,
                "guildId": str(guild_id),
                "sessionId": session_id,
                "channelId": str(channel_id),
                "event": event,
            }
        )

    async def destroy_guild(self, guild_id: int):
        await self.send({"op": LavalinkOutgoingOp.DESTROY.value, "guildId": str(guild_id)})

    async def no_event_stop(self, guild_id: int):
        await self.send({"op": LavalinkOutgoingOp.STOP.value, "guildId": str(guild_id)})

    # Player commands
    async def stop(self, guild_id: int):
        await self.no_event_stop(guild_id=guild_id)
        self.event_handler(
            LavalinkIncomingOp.EVENT, LavalinkEvents.QUEUE_END, {"guildId": str(guild_id)}
        )

    async def no_stop_play(
        self,
        guild_id: int,
        track: Track,
        replace: bool = True,
        start: int = 0,
        pause: bool = False,
    ):
        await self.send(
            {
                "op": LavalinkOutgoingOp.PLAY.value,
                "guildId": str(guild_id),
                "track": track.track_identifier,
                "noReplace": not replace,
                "startTime": str(start),
                "pause": pause,
            }
        )

    async def play(
        self,
        guild_id: int,
        track: Track,
        replace: bool = True,
        start: int = 0,
        pause: bool = False,
    ):
        # await self.send({"op": LavalinkOutgoingOp.STOP.value, "guildId": str(guild_id)})
        await self.no_stop_play(
            guild_id=guild_id, track=track, replace=replace, start=start, pause=pause
        )

    async def pause(self, guild_id, paused):
        await self.send(
            {"op": LavalinkOutgoingOp.PAUSE.value, "guildId": str(guild_id), "pause": paused}
        )

    async def volume(self, guild_id: int, _volume: int):
        await self.send(
            {"op": LavalinkOutgoingOp.VOLUME.value, "guildId": str(guild_id), "volume": _volume}
        )

    async def seek(self, guild_id: int, position: int):
        await self.send(
            {"op": LavalinkOutgoingOp.SEEK.value, "guildId": str(guild_id), "position": position}
        )


def get_node(guild_id: int = None, *, ignore_ready_status: bool = False) -> Node:
    """
    Gets a node based on a guild ID, useful for noding separation. If the
    guild ID does not already have a node association, the least used
    node is returned. Skips over nodes that are not yet ready.

    Parameters
    ----------
    guild_id : int
    ignore_ready_status : bool

    Returns
    -------
    Node
    """
    guild_count = 1e10
    least_used = None

    for node in _nodes:
        guild_ids = node.guild_ids

        if ignore_ready_status is False and not node.ready:
            continue
        elif len(guild_ids) < guild_count:
            guild_count = len(guild_ids)
            least_used = node

        if guild_id in guild_ids:
            return node

    if least_used is None:
        raise NodeNotFound("No Lavalink nodes found.")

    return least_used


def get_nodes_stats():
    return [node.stats for node in _nodes]


def get_all_nodes() -> List[Node]:
    return [node for node in _nodes]


async def disconnect():
    for node in _nodes.copy():
        await node.disconnect()
