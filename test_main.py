import json
from unittest.mock import MagicMock, patch

import pytest

from main import (
    Settings,
    insert_flattened_games,
    load_json_files,
    publish_event,
    start_consumer,
)


@pytest.fixture
def settings(tmp_path):
    """Provide test settings with a temporary directory."""
    s = Settings()
    s.GAMES_FLATTEN_DIR = tmp_path
    return s


@pytest.fixture
def fake_clickhouse_client():
    """Mock ClickHouse client."""
    client = MagicMock()
    client.insert = MagicMock()
    return client


@pytest.fixture
def fake_channel():
    """Mock RabbitMQ channel."""
    ch = MagicMock()
    ch.basic_publish = MagicMock()
    ch.basic_ack = MagicMock()
    return ch


# -----------------------------
# load_json_files
# -----------------------------


def test_load_json_files_valid(tmp_path):
    file = tmp_path / "game.json"
    file.write_text(json.dumps([{"game_id": 1, "begin_at": "2024-01-01T00:00:00"}]))
    result = list(load_json_files(tmp_path))
    assert len(result) == 1
    assert result[0][0]["game_id"] == 1


def test_load_json_files_invalid_json(tmp_path):
    file = tmp_path / "bad.json"
    file.write_text("{not_valid_json")
    result = list(load_json_files(tmp_path))
    assert result == []  # Should skip invalid


# -----------------------------
# insert_flattened_games
# -----------------------------


def test_insert_flattened_games_inserts(fake_clickhouse_client, settings, tmp_path):
    data = [
        {
            "game_id": 101,
            "begin_at": "2024-01-01T10:00:00",
            "kills": 10,
            "deaths": 5,
            "adr": 90.5,
        }
    ]
    file = tmp_path / "one_game.json"
    file.write_text(json.dumps(data))
    settings.GAMES_FLATTEN_DIR = tmp_path

    inserted = insert_flattened_games(fake_clickhouse_client, settings)

    fake_clickhouse_client.insert.assert_called_once()
    assert inserted == 1


def test_insert_flattened_games_skips_invalid(
    fake_clickhouse_client, settings, tmp_path
):
    data = [{"bad": "no game_id"}]
    file = tmp_path / "bad_game.json"
    file.write_text(json.dumps(data))
    settings.GAMES_FLATTEN_DIR = tmp_path

    inserted = insert_flattened_games(fake_clickhouse_client, settings)
    fake_clickhouse_client.insert.assert_not_called()
    assert inserted == 0


# -----------------------------
# publish_event
# -----------------------------


def test_publish_event(fake_channel, settings):
    publish_event(fake_channel, settings)
    fake_channel.basic_publish.assert_called_once()
    args, kwargs = fake_channel.basic_publish.call_args
    body = json.loads(kwargs["body"])
    assert "event_uuid" in body
    assert body["event_type"] == settings.RABBITMQ_ROUTING_KEY_WRITE


# -----------------------------
# start_consumer
# -----------------------------


def test_start_consumer_processes_message(
    fake_clickhouse_client, fake_channel, settings
):
    """Simulate receiving one message and verify callback is called properly."""

    # Prepare a fake method object for ACK
    method = MagicMock()
    method.delivery_tag = "tag1"

    # Mock `basic_consume` to capture the callback
    captured_callback = {}

    def fake_basic_consume(queue, on_message_callback):
        captured_callback["fn"] = on_message_callback

    fake_channel.basic_consume.side_effect = fake_basic_consume

    # Mock start_consuming to stop after one call
    fake_channel.start_consuming.side_effect = KeyboardInterrupt

    with (
        patch("main.insert_flattened_games", return_value=3) as mock_insert,
        patch("main.publish_event") as mock_publish,
    ):
        start_consumer(settings, fake_clickhouse_client, fake_channel)

        # Simulate message callback manually
        callback_fn = captured_callback["fn"]
        callback_fn(fake_channel, method, None, b"{}")

        mock_insert.assert_called_once()
        mock_publish.assert_called_once()
        fake_channel.basic_ack.assert_called_once_with(delivery_tag="tag1")
