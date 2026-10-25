from lavalink.node import Node

_ORIGINAL_DETECT_MAJOR_VERSION = Node._detect_major_version


class _FakeResponse:
    def __init__(self, status, data):
        self.status = status
        self._data = data

    async def json(self, content_type=None):
        return self._data


class _FakeGet:
    def __init__(self, status, data):
        self._status = status
        self._data = data

    async def __aenter__(self):
        return _FakeResponse(self._status, self._data)

    async def __aexit__(self, *args):
        return False


class _FakeSession:
    def __init__(self, status, data):
        self._status = status
        self._data = data
        self.url = None

    def get(self, url, **kwargs):
        self.url = url
        return _FakeGet(self._status, self._data)


def _node_with_session(session):
    node = Node.__new__(Node)
    node.host = "lavalink.example.com"
    node.port = 443
    node.secured = True
    node.password = "password"
    node.session = session
    node._major_version = None
    return node


async def test_detect_major_version_v4():
    session = _FakeSession(200, {"version": {"major": 0, "semver": "snapshot"}})
    node = _node_with_session(session)

    await _ORIGINAL_DETECT_MAJOR_VERSION(node)

    assert node.protocol_version == 4
    assert session.url == "https://lavalink.example.com:443/v4/info"


async def test_detect_major_version_v3():
    session = _FakeSession(404, {})
    node = _node_with_session(session)

    await _ORIGINAL_DETECT_MAJOR_VERSION(node)

    assert node.protocol_version == 3


def test_stats_with_null_frame_stats():
    from lavalink.node import NodeStats

    stats = NodeStats(
        {
            "uptime": 1,
            "players": 1,
            "playingPlayers": 1,
            "memory": {"free": 1, "used": 1, "allocated": 1, "reservable": 1},
            "cpu": {"cores": 1, "systemLoad": 0.1, "lavalinkLoad": 0.1},
            "frameStats": None,
        }
    )

    assert stats.frames_sent == -1
    assert stats.frames_nulled == -1
    assert stats.frames_deficit == -1
