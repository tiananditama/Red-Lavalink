import re
from collections import namedtuple
from typing import Dict, Tuple, Union
from urllib.parse import parse_qsl, quote, urlencode, urlparse, urlunparse

import aiohttp
import discord
from aiohttp.client_exceptions import ServerDisconnectedError

from . import log
from .enums import ExceptionSeverity, LoadType, PlayerState
from .utils import VoiceChannel

__all__ = ("Track", "RESTClient", "PlaylistInfo")

_PlaylistInfo = namedtuple("PlaylistInfo", "name selectedTrack")


# This exists to preprocess rather than pull in dataclasses for __post_init__
def PlaylistInfo(name=None, selectedTrack=None):
    return _PlaylistInfo(
        name if name is not None else "Unknown",
        selectedTrack if selectedTrack is not None else -1,
    )


_re_youtube_timestamp = re.compile(r"[&?]t=(\d+)s?")
_re_soundcloud_timestamp = re.compile(r"#t=(\d+):(\d+)s?")
_re_twitch_timestamp = re.compile(r"\?t=(\d+)h(\d+)m(\d+)s")


def parse_timestamps(data):
    if data["loadType"] == LoadType.PLAYLIST_LOADED:
        return data["tracks"]

    new_tracks = []
    query = data["query"]
    try:
        query_url = urlparse(query)
    except:
        query_url = None
    if not query_url:
        return data["tracks"]

    for track in data["tracks"]:
        start_time = 0
        try:
            if all([query_url.scheme, query_url.netloc, query_url.path]) or any(
                x in query for x in ["ytsearch:", "scsearch:"]
            ):
                url_domain = ".".join(query_url.netloc.split(".")[-2:])
                if not query_url.netloc:
                    url_domain = ".".join(query_url.path.split("/")[0].split(".")[-2:])
                if (
                    (url_domain in ["youtube.com", "youtu.be"] or "ytsearch:" in query)
                    and any(x in query for x in ["&t=", "?t="])
                    and not all(k in query for k in ["playlist?", "&list="])
                ):
                    match = re.search(_re_youtube_timestamp, query)
                    if match:
                        start_time = int(match.group(1))
                elif (url_domain == "soundcloud.com" or "scsearch:" in query) and "#t=" in query:
                    if "/sets/" not in query or ("/sets/" in query and "?in=" in query):
                        match = re.search(_re_soundcloud_timestamp, query)
                        if match:
                            start_time = (int(match.group(1)) * 60) + int(match.group(2))
                elif url_domain == "twitch.tv" and "?t=" in query:
                    match = re.search(_re_twitch_timestamp, query)
                    if match:
                        start_time = (
                            (int(match.group(1)) * 60 * 60)
                            + (int(match.group(2)) * 60)
                            + int(match.group(3))
                        )
        except Exception:
            pass
        track["info"]["timestamp"] = start_time * 1000
        new_tracks.append(track)
    return new_tracks


def reformat_query(query):
    try:
        query_url = urlparse(query)
        if all([query_url.scheme, query_url.netloc, query_url.path]) or any(
            x in query for x in ["ytsearch:", "scsearch:"]
        ):
            url_domain = ".".join(query_url.netloc.split(".")[-2:])
            if not query_url.netloc:
                url_domain = ".".join(query_url.path.split("/")[0].split(".")[-2:])
            if (
                (url_domain in ["youtube.com", "youtu.be"] or "ytsearch:" in query)
                and any(x in query for x in ["&t=", "?t="])
                and not all(k in query for k in ["playlist?", "&list="])
            ):
                match = re.search(_re_youtube_timestamp, query)
                if match:
                    query = query.split("&t=")[0].split("?t=")[0]
            elif (url_domain == "soundcloud.com" or "scsearch:" in query) and "#t=" in query:
                if "/sets/" not in query or ("/sets/" in query and "?in=" in query):
                    match = re.search(_re_soundcloud_timestamp, query)
                    if match:
                        query = query.split("#t=")[0]
            elif url_domain == "twitch.tv" and "?t=" in query:
                match = re.search(_re_twitch_timestamp, query)
                if match:
                    query = query.split("?t=")[0]
    except Exception:
        pass
    return query


_V4_LOAD_TYPES = {"track", "playlist", "search", "empty", "error"}

_YOUTUBE_PLAYLIST_PARAMS = {"list", "index", "start_radio", "pp"}


def strip_youtube_playlist_params(query: str) -> str:
    """Remove playlist parameters from a YouTube video URL.

    YouTube watch links copied from a playlist carry ``list``/``index`` parameters.
    Lavalink then tries to load the playlist instead of the video, which fails
    entirely when that playlist is private or deleted. Dropping those parameters
    lets the same link play the video on its own.

    Pure playlist URLs and non-YouTube URLs are returned unchanged.
    """
    try:
        parsed = urlparse(query)
    except ValueError:
        return query

    if not parsed.netloc:
        return query

    url_domain = ".".join(parsed.netloc.split(".")[-2:])
    if url_domain not in ("youtube.com", "youtu.be"):
        return query

    params = parse_qsl(parsed.query, keep_blank_values=True)
    if not any(key == "list" for key, _ in params):
        return query

    kept = [(key, value) for key, value in params if key not in _YOUTUBE_PLAYLIST_PARAMS]
    is_short_link = parsed.netloc.endswith("youtu.be")
    if not is_short_link and not any(key == "v" for key, _ in kept):
        return query

    return urlunparse(parsed._replace(query=urlencode(kept)))


def _adapt_v4_track(track) -> dict:
    if isinstance(track, str):
        return {"track": track, "info": {}}
    if not isinstance(track, dict):
        return {"track": None, "info": {}}
    return {
        "track": track.get("encoded") or track.get("track"),
        "info": track.get("info") or {},
        "extras": track.get("userData") or track.get("extras") or {},
    }


def adapt_v4_load_result(data):
    """Convert a Lavalink v4 loadtracks response to the v3 shape used by this library."""
    if not isinstance(data, dict):
        return data
    load_type = data.get("loadType")
    if load_type not in _V4_LOAD_TYPES:
        return data

    payload = data.get("data")
    if load_type == "track":
        tracks = [_adapt_v4_track(payload)] if payload else []
        return {"loadType": LoadType.TRACK_LOADED, "tracks": tracks, "playlistInfo": {}}
    if load_type == "playlist":
        payload = payload or {}
        info = payload.get("info") or {}
        return {
            "loadType": LoadType.PLAYLIST_LOADED,
            "tracks": [_adapt_v4_track(track) for track in payload.get("tracks") or []],
            "playlistInfo": {
                "name": info.get("name"),
                "selectedTrack": info.get("selectedTrack", -1),
            },
        }
    if load_type == "search":
        return {
            "loadType": LoadType.SEARCH_RESULT,
            "tracks": [_adapt_v4_track(track) for track in payload or []],
            "playlistInfo": {},
        }
    if load_type == "empty":
        return {"loadType": LoadType.NO_MATCHES, "tracks": [], "playlistInfo": {}}

    payload = payload or {}
    return {
        "loadType": LoadType.LOAD_FAILED,
        "tracks": [],
        "playlistInfo": {},
        "exception": {
            "message": payload.get("message") or "",
            "cause": payload.get("cause") or "",
            "severity": str(payload.get("severity") or "fault").upper(),
        },
    }


class Track:
    """
    Information about a Lavalink track.

    Attributes
    ----------
    requester : discord.User
        The user who requested the track.
    track_identifier : str
        Track identifier used by the Lavalink player to play tracks.
    seekable : bool
        Boolean determining if seeking can be done on this track.
    author : str
        The author of this track.
    length : int
        The length of this track in milliseconds.
    is_stream : bool
        Determines whether Lavalink will stream this track.
    position : int
        Current seeked position to begin playback.
    title : str
        Title of this track.
    uri : str
        The playback url of this track.
    start_timestamp: int
        The track start time in milliseconds as provided by the query.
    """

    def __init__(self, data):
        self.requester = None

        self.track_identifier = data.get("encoded") or data.get("track")
        self._info = data.get("info", {})
        self.seekable = self._info.get("isSeekable", False)
        self.author = self._info.get("author")
        self.length = self._info.get("length", 0)
        self.is_stream = self._info.get("isStream", False)
        self.position = self._info.get("position")
        self.title = self._info.get("title")
        self.uri = self._info.get("uri")
        self.start_timestamp = self._info.get("timestamp", 0)
        self.extras = data.get("userData") or data.get("extras", {})
        self._artwork_url = self._info.get("artworkUrl")

    @property
    def thumbnail(self):
        """Optional[str]: Returns a thumbnail URL for the track, if available."""
        if self._artwork_url:
            return self._artwork_url
        if self.uri and "youtube" in self.uri and "identifier" in self._info:
            return "https://img.youtube.com/vi/{}/mqdefault.jpg".format(self._info["identifier"])

    def __eq__(self, other):
        """Overrides the default implementation"""
        if isinstance(other, Track):
            return self.track_identifier == other.track_identifier
        return NotImplemented

    def __ne__(self, other):
        """Overrides the default implementation"""
        x = self.__eq__(other)
        if x is not NotImplemented:
            return not x
        return NotImplemented

    def __hash__(self):
        """Overrides the default implementation"""
        return hash((self.track_identifier, self.title, self.author, self.uri))

    def __repr__(self):
        return (
            "<Track: "
            f"track_identifier={self.track_identifier!r}, "
            f"author={self.author!r}, "
            f"length={self.length}, "
            f"is_stream={self.is_stream}, uri={self.uri!r}, title={self.title!r}>"
        )


class LoadResult:
    """
    The result of a load_tracks request.

    Attributes
    ----------
    load_type : LoadType
        The result of the loadtracks request
    playlist_info : Optional[PlaylistInfo]
        The playlist information detected by Lavalink
    tracks : Tuple[Track, ...]
        The tracks that were loaded, if any
    """

    def __init__(self, data):
        self._raw = data
        _fallback = {
            "loadType": LoadType.LOAD_FAILED,
            "exception": {
                "message": "Lavalink API returned an unsupported response, Please report it.",
                "severity": ExceptionSeverity.SUSPICIOUS,
            },
            "playlistInfo": {},
            "tracks": [],
        }
        for k, v in _fallback.items():
            if k not in data:
                if (
                    k == "exception"
                    and data.get("loadType", LoadType.LOAD_FAILED) != LoadType.LOAD_FAILED
                ):
                    continue
                elif k == "exception":
                    v["message"] = (
                        f"Timestamp: {self._raw.get('timestamp', 'Unknown')}\n"
                        f"Status Code: {self._raw.get('status', 'Unknown')}\n"
                        f"Error: {self._raw.get('error', 'Unknown')}\n"
                        f"Query: {self._raw.get('query', 'Unknown')}\n"
                        f"Load Type: {self._raw['loadType']}\n"
                        f"Message: {self._raw.get('message', v['message'])}"
                    )
                self._raw.update({k: v})

        self.load_type = LoadType(self._raw["loadType"])

        is_playlist = self._raw.get("isPlaylist") or self.load_type == LoadType.PLAYLIST_LOADED
        if is_playlist is True:
            self.is_playlist = True
            self.playlist_info = PlaylistInfo(**self._raw["playlistInfo"])
        elif is_playlist is False:
            self.is_playlist = False
            self.playlist_info = None
        else:
            self.is_playlist = None
            self.playlist_info = None
        _tracks = parse_timestamps(self._raw) if self._raw.get("query") else self._raw["tracks"]
        self.tracks = tuple(Track(t) for t in _tracks)

    @property
    def has_error(self):
        return self.load_type == LoadType.LOAD_FAILED

    @property
    def exception_message(self) -> Union[str, None]:
        """
        On Lavalink V3, if there was an exception during a load or get tracks call
        this property will be populated with the error message.
        If there was no error this property will be ``None``.
        """
        if self.has_error:
            exception_data = self._raw.get("exception", {})
            return exception_data.get("message")
        return None

    @property
    def exception_severity(self) -> Union[ExceptionSeverity, None]:
        if self.has_error:
            exception_data = self._raw.get("exception", {})
            severity = exception_data.get("severity")
            if severity is not None:
                return ExceptionSeverity(severity)
        return None


class RESTClient:
    """
    Client class used to access the REST endpoints on a Lavalink node.
    """

    def __init__(self, client: discord.Client, channel: VoiceChannel):
        from lavalink.node import get_node

        self.node = get_node()
        self.client = client
        self.state: PlayerState = PlayerState.CREATED
        self.channel: discord.VoiceChannel = channel
        self.guild: discord.Guild = channel.guild
        self._last_channel_id = channel.id
        self.secured: bool = self.node.secured
        self._session: aiohttp.ClientSession = self.node.session
        if self.secured:
            protocol = "https"
        else:
            protocol = "http"
        self._is_v4: bool = self.node.protocol_version >= 4
        loadtracks_path = (
            "/v4/loadtracks?identifier=" if self._is_v4 else "/loadtracks?identifier="
        )
        self._uri: str = f"{protocol}://{self.node.host}:{self.node.port}{loadtracks_path}"
        self._headers: Dict[str, str] = {"Authorization": self.node.password}
        self._warned: bool = False

    def __check_node_ready(self):
        if self.state != PlayerState.READY:
            raise RuntimeError("Cannot execute REST request when node not ready.")

    async def _get(self, url):
        try:
            async with self._session.get(url, headers=self._headers) as resp:
                data = await resp.json(content_type=None)
        except ServerDisconnectedError:
            if self.state == PlayerState.DISCONNECTING:
                return {
                    "loadType": LoadType.LOAD_FAILED,
                    "exception": {
                        "message": "Load tracks interrupted by player disconnect.",
                        "severity": ExceptionSeverity.COMMON,
                    },
                    "tracks": [],
                }
            log.debug("Received server disconnected error when player state = %s", self.state.name)
            raise
        return data

    async def _load_once(self, identifier: str, timestamp_query: str) -> LoadResult:
        url = self._uri + quote(identifier)
        data = await self._get(url)
        if self._is_v4:
            data = adapt_v4_load_result(data)
        if isinstance(data, dict):
            data = dict(data)
            data["query"] = timestamp_query
            data["encodedquery"] = url
            return LoadResult(data)
        elif isinstance(data, list):
            modified_data = {
                "loadType": LoadType.V2_COMPAT,
                "tracks": data,
                "query": timestamp_query,
                "encodedquery": url,
            }
            return LoadResult(modified_data)
        return LoadResult({"loadType": LoadType.LOAD_FAILED, "tracks": [], "playlistInfo": {}})

    async def load_tracks(self, query) -> LoadResult:
        """
        Executes a loadtracks request. Supports Lavalink V3 and V4.

        If a YouTube video link that carries playlist parameters fails to load,
        it is retried without those parameters so the video still plays.

        Parameters
        ----------
        query : str

        Returns
        -------
        LoadResult
        """
        self.__check_node_ready()
        _raw_url = str(query)
        parsed_url = reformat_query(_raw_url)
        result = await self._load_once(parsed_url, _raw_url)
        if result.has_error or not result.tracks:
            fallback = strip_youtube_playlist_params(parsed_url)
            if fallback != parsed_url:
                log.debug("Retrying track load without playlist parameters: %s", fallback)
                retry = await self._load_once(fallback, fallback)
                if not retry.has_error and retry.tracks:
                    return retry
        return result

    async def get_tracks(self, query) -> Tuple[Track, ...]:
        """
        Gets tracks from lavalink.

        Parameters
        ----------
        query : str

        Returns
        -------
        Tuple[Track, ...]
        """
        if not self._warned:
            log.warn("get_tracks() is now deprecated. Please switch to using load_tracks().")
            self._warned = True
        result = await self.load_tracks(query)
        return result.tracks

    async def search_yt(self, query) -> LoadResult:
        """
        Gets track results from YouTube from Lavalink.

        Parameters
        ----------
        query : str

        Returns
        -------
        list of Track
        """
        return await self.load_tracks("ytsearch:{}".format(query))

    async def search_sc(self, query) -> LoadResult:
        """
        Gets track results from SoundCloud from Lavalink.

        Parameters
        ----------
        query : str

        Returns
        -------
        list of Track
        """
        return await self.load_tracks("scsearch:{}".format(query))
