from lavalink.enums import LavalinkEvents, TrackEndReason
from lavalink.player import Player


def _player_with_current(track, repeat_track):
    player = Player.__new__(Player)
    player.current = track
    player.queue = []
    player.repeat_track = repeat_track
    player.played = []

    async def fake_play():
        player.played.append(True)

    player.play = fake_play
    return player


async def test_repeat_track_requeues_current_on_finish():
    track = object()
    player = _player_with_current(track, repeat_track=True)

    await player.handle_event(LavalinkEvents.TRACK_END, TrackEndReason.FINISHED)

    assert player.queue == [track]
    assert player.played == [True]


async def test_repeat_track_off_does_not_requeue():
    track = object()
    player = _player_with_current(track, repeat_track=False)

    await player.handle_event(LavalinkEvents.TRACK_END, TrackEndReason.FINISHED)

    assert player.queue == []
    assert player.played == [True]


async def test_repeat_track_does_not_requeue_on_stop_reason():
    track = object()
    player = _player_with_current(track, repeat_track=True)

    await player.handle_event(LavalinkEvents.TRACK_END, TrackEndReason.STOPPED)

    assert player.queue == []
    assert player.played == []
