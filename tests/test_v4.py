from lavalink.enums import LoadType
from lavalink.node import Node
from lavalink.rest_api import Track, adapt_v4_load_result


def _v4_track(encoded="encoded-track", **info):
    track_info = {
        "identifier": "identifier",
        "isSeekable": True,
        "author": "Author",
        "length": 1000,
        "isStream": False,
        "position": 0,
        "title": "Title",
        "uri": "https://example.com/track",
        "artworkUrl": "https://example.com/artwork.jpg",
        "isrc": None,
        "sourceName": "youtube",
    }
    track_info.update(info)
    return {
        "encoded": encoded,
        "info": track_info,
        "pluginInfo": {},
        "userData": {"key": "value"},
    }


def test_adapt_v4_track_result():
    result = adapt_v4_load_result({"loadType": "track", "data": _v4_track()})

    assert result["loadType"] is LoadType.TRACK_LOADED
    track = Track(result["tracks"][0])
    assert track.track_identifier == "encoded-track"
    assert track.title == "Title"
    assert track.author == "Author"
    assert track.uri == "https://example.com/track"
    assert track.extras == {"key": "value"}
    assert track.thumbnail == "https://example.com/artwork.jpg"


def test_adapt_v4_search_result():
    result = adapt_v4_load_result(
        {"loadType": "search", "data": [_v4_track("one"), _v4_track("two")]}
    )

    assert result["loadType"] is LoadType.SEARCH_RESULT
    assert [track["track"] for track in result["tracks"]] == ["one", "two"]


def test_adapt_v4_playlist_result():
    result = adapt_v4_load_result(
        {
            "loadType": "playlist",
            "data": {
                "info": {"name": "Playlist", "selectedTrack": 0},
                "tracks": [_v4_track("one")],
            },
        }
    )

    assert result["loadType"] is LoadType.PLAYLIST_LOADED
    assert result["playlistInfo"] == {"name": "Playlist", "selectedTrack": 0}
    assert result["tracks"][0]["track"] == "one"


def test_adapt_v4_empty_result():
    result = adapt_v4_load_result({"loadType": "empty", "data": None})

    assert result["loadType"] is LoadType.NO_MATCHES
    assert result["tracks"] == []


def test_adapt_v4_error_result():
    result = adapt_v4_load_result(
        {
            "loadType": "error",
            "data": {"message": "Bad query", "severity": "common", "cause": "invalid"},
        }
    )

    assert result["loadType"] is LoadType.LOAD_FAILED
    assert result["exception"] == {
        "message": "Bad query",
        "cause": "invalid",
        "severity": "COMMON",
    }


def test_adapt_v3_result_is_unchanged():
    data = {"loadType": "TRACK_LOADED", "tracks": [], "playlistInfo": {}}

    assert adapt_v4_load_result(data) is data


def test_normalize_v4_track_end_event():
    node = Node.__new__(Node)
    normalized = node._normalize_v4_event(
        {
            "op": "event",
            "type": "TrackEndEvent",
            "guildId": "123",
            "track": _v4_track(),
            "reason": "loadFailed",
        }
    )

    assert normalized["reason"] == "LOAD_FAILED"
    assert normalized["track"] == "encoded-track"


def test_normalize_v4_track_start_event():
    node = Node.__new__(Node)
    normalized = node._normalize_v4_event(
        {"op": "event", "type": "TrackStartEvent", "guildId": "123", "track": _v4_track()}
    )

    assert normalized["track"] == "encoded-track"


def test_normalize_v4_exception_event():
    node = Node.__new__(Node)
    normalized = node._normalize_v4_event(
        {
            "op": "event",
            "type": "TrackExceptionEvent",
            "guildId": "123",
            "track": _v4_track(),
            "exception": {"message": None, "severity": "fault", "cause": None},
        }
    )

    assert normalized["exception"] == {
        "message": "",
        "severity": "FAULT",
        "cause": "",
    }


def test_normalize_v4_websocket_closed_reason_not_mapped():
    node = Node.__new__(Node)
    normalized = node._normalize_v4_event(
        {
            "op": "event",
            "type": "WebSocketClosedEvent",
            "guildId": "123",
            "code": 4006,
            "reason": "Your session is no longer valid.",
            "byRemote": True,
        }
    )

    assert normalized["reason"] == "Your session is no longer valid."


class _RestRecorder:
    def __init__(self):
        self.calls = []

    async def __call__(self, method, path, *, params=None, json=None):
        self.calls.append((method, path, params, json))
        return None


def _node_for_send():
    node = Node.__new__(Node)
    node._session_id = "session-id"
    node._queue = []
    node._filters = {}
    return node


async def test_send_v4_play():
    node = _node_for_send()
    recorder = _RestRecorder()
    node._rest_request = recorder

    await node._send_v4(
        {
            "op": "play",
            "guildId": "123",
            "track": "encoded",
            "startTime": "5000",
            "pause": True,
            "noReplace": True,
        }
    )

    assert recorder.calls == [
        (
            "PATCH",
            "/v4/sessions/session-id/players/123",
            {"noReplace": "true"},
            {"track": {"encoded": "encoded"}, "position": 5000, "paused": True},
        )
    ]


async def test_send_v4_stop_pause_seek_volume():
    node = _node_for_send()
    recorder = _RestRecorder()
    node._rest_request = recorder

    await node._send_v4({"op": "stop", "guildId": "1"})
    await node._send_v4({"op": "pause", "guildId": "1", "pause": True})
    await node._send_v4({"op": "seek", "guildId": "1", "position": 250})
    await node._send_v4({"op": "volume", "guildId": "1", "volume": 50})

    assert recorder.calls == [
        ("PATCH", "/v4/sessions/session-id/players/1", None, {"track": {"encoded": None}}),
        ("PATCH", "/v4/sessions/session-id/players/1", None, {"paused": True}),
        ("PATCH", "/v4/sessions/session-id/players/1", None, {"position": 250}),
        ("PATCH", "/v4/sessions/session-id/players/1", None, {"volume": 50}),
    ]


async def test_send_v4_equalizer_uses_filters():
    node = _node_for_send()
    recorder = _RestRecorder()
    node._rest_request = recorder
    bands = [{"band": 0, "gain": 0.2}]

    await node._send_v4({"op": "equalizer", "guildId": "1", "bands": bands})

    assert recorder.calls == [
        ("PATCH", "/v4/sessions/session-id/players/1", None, {"filters": {"equalizer": bands}})
    ]
    assert node._filters == {"1": {"equalizer": bands}}


async def test_send_v4_voice_update():
    node = _node_for_send()
    recorder = _RestRecorder()
    node._rest_request = recorder

    await node._send_v4(
        {
            "op": "voiceUpdate",
            "guildId": "1",
            "sessionId": "discord-voice-session",
            "channelId": "channel-id",
            "event": {"token": "token", "endpoint": "endpoint"},
        }
    )

    assert recorder.calls == [
        (
            "PATCH",
            "/v4/sessions/session-id/players/1",
            None,
            {
                "voice": {
                    "token": "token",
                    "endpoint": "endpoint",
                    "sessionId": "discord-voice-session",
                    "channelId": "channel-id",
                }
            },
        )
    ]


async def test_send_v4_destroy():
    node = _node_for_send()
    recorder = _RestRecorder()
    node._rest_request = recorder

    await node._send_v4({"op": "destroy", "guildId": "1"})

    assert recorder.calls == [("DELETE", "/v4/sessions/session-id/players/1", None, None)]


async def test_send_v4_queues_until_session_ready():
    node = _node_for_send()
    node._session_id = None
    recorder = _RestRecorder()
    node._rest_request = recorder

    await node._send_v4({"op": "pause", "guildId": "1", "pause": True})

    assert recorder.calls == []
    assert node._queue == [{"op": "pause", "guildId": "1", "pause": True}]
