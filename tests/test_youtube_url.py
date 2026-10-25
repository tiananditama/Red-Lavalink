from urllib.parse import unquote

from lavalink.enums import LoadType, PlayerState
from lavalink.rest_api import RESTClient, adapt_v4_load_result, strip_youtube_playlist_params


def test_strips_playlist_params_from_watch_url():
    query = "https://www.youtube.com/watch?v=dQw4w9WgXcQ&list=PL123&index=2&start_radio=1"

    assert strip_youtube_playlist_params(query) == "https://www.youtube.com/watch?v=dQw4w9WgXcQ"


def test_strips_playlist_params_from_short_link():
    query = "https://youtu.be/dQw4w9WgXcQ?list=PL123&index=4"

    assert strip_youtube_playlist_params(query) == "https://youtu.be/dQw4w9WgXcQ"


def test_keeps_timestamp_parameter():
    query = "https://www.youtube.com/watch?v=dQw4w9WgXcQ&list=PL123&t=42"

    assert (
        strip_youtube_playlist_params(query) == "https://www.youtube.com/watch?v=dQw4w9WgXcQ&t=42"
    )


def test_pure_playlist_url_is_unchanged():
    query = "https://www.youtube.com/playlist?list=PL123"

    assert strip_youtube_playlist_params(query) == query


def test_non_youtube_url_is_unchanged():
    query = "https://example.com/watch?v=abc&list=PL123"

    assert strip_youtube_playlist_params(query) == query


def test_url_without_playlist_is_unchanged():
    query = "https://www.youtube.com/watch?v=dQw4w9WgXcQ&t=30"

    assert strip_youtube_playlist_params(query) == query


def _rest_client(responses):
    client = RESTClient.__new__(RESTClient)
    client.state = PlayerState.READY
    client._is_v4 = True
    client._uri = "https://lavalink.example.com/loadtracks?identifier="
    client.requested = []

    async def fake_get(url):
        client.requested.append(url)
        return responses.pop(0)

    client._get = fake_get
    return client


async def test_load_tracks_falls_back_to_video_when_playlist_fails():
    encoded = {"encoded": "track-data", "info": {"title": "Track"}}
    client = _rest_client(
        [
            {
                "loadType": "error",
                "data": {"message": "The playlist does not exist.", "severity": "common"},
            },
            {"loadType": "track", "data": encoded},
        ]
    )

    result = await client.load_tracks(
        "https://www.youtube.com/watch?v=dQw4w9WgXcQ&list=PL123&index=2"
    )

    assert result.load_type is LoadType.TRACK_LOADED
    assert result.tracks[0].track_identifier == "track-data"
    assert len(client.requested) == 2
    assert "list=" not in unquote(client.requested[1])


async def test_load_tracks_keeps_playlist_when_it_loads():
    encoded = {"encoded": "track-data", "info": {"title": "Track"}}
    client = _rest_client([{"loadType": "track", "data": encoded}])

    result = await client.load_tracks("https://www.youtube.com/watch?v=dQw4w9WgXcQ&list=PL123")

    assert result.load_type is LoadType.TRACK_LOADED
    assert len(client.requested) == 1
    assert "list=" in unquote(client.requested[0])
