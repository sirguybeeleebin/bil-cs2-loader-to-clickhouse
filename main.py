import argparse
import json
import logging
import uuid
from pathlib import Path
from typing import Literal

import pika
from clickhouse_connect import get_client
from dateutil.parser import parse
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    APP_LOG_LEVEL: Literal["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"] = "INFO"

    CLICKHOUSE_HOST: str = "localhost"
    CLICKHOUSE_PORT: int = 8123
    CLICKHOUSE_USER: str = "default"
    CLICKHOUSE_PASSWORD: str = ""
    CLICKHOUSE_DATABASE: str = "cs2"
    CLICKHOUSE_TABLE: str = "games_flatten"

    GAMES_FLATTEN_DIR: Path = Path("../bil-cs2-data/games_flatten")

    RABBITMQ_URL: str = "amqp://guest:guest@localhost:5672/"
    RABBITMQ_EXCHANGE: str = "cs2_events"
    RABBITMQ_QUEUE_NAME: str = "cs2_events_queue"
    RABBITMQ_ROUTING_KEY_READ: str = "all_games_parsed"
    RABBITMQ_ROUTING_KEY_WRITE: str = "games_loaded_to_clickhouse"


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS {db}.{table} (
    game_flatten_uuid UUID DEFAULT generateUUIDv4(),
    game_id UInt64,
    begin_at DateTime,
    map_id UInt32,
    league_id UInt32,
    serie_id UInt32,
    serie_tier UInt8,
    tournament_id UInt32,
    team_id UInt32,
    team_opponent_id UInt32,
    player_id UInt32,
    player_opponent_id UInt32,
    adr Float32,
    kast Float32,
    rating Float32,
    kills UInt16,
    deaths UInt16,
    assists UInt16,
    headshots UInt16,
    flash_assists UInt16,
    first_kills_diff Int16,
    k_d_diff Int16,
    round UInt8,
    is_ct UInt8,
    outcome UInt8,
    win UInt8
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(begin_at)
ORDER BY game_id
SETTINGS index_granularity = 8192, compression_codec = 'ZSTD(3)';
"""

log = logging.getLogger(__name__)


# ------------------------
# Setup & Helpers
# ------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="CS2 Loader to ClickHouse")
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    return parser.parse_args()


def parse_env_file(env_file: Path) -> Settings:
    return Settings(_env_file=env_file)


def configure_logger(settings: Settings):
    logging.basicConfig(
        level=getattr(logging, settings.APP_LOG_LEVEL.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def load_json_files(path_to_dir: Path):
    for file_path in path_to_dir.glob("*.json"):
        try:
            with file_path.open("r", encoding="utf-8") as f:
                yield json.load(f)
        except Exception as e:
            log.warning("Skipping %s: %s", file_path.name, e)


# ------------------------
# Dependency Setup
# ------------------------


def connect_clickhouse(settings: Settings):
    client = get_client(
        host=settings.CLICKHOUSE_HOST,
        port=settings.CLICKHOUSE_PORT,
        username=settings.CLICKHOUSE_USER,
        password=settings.CLICKHOUSE_PASSWORD,
        database=settings.CLICKHOUSE_DATABASE,
    )
    client.command(
        CREATE_TABLE_SQL.format(
            db=settings.CLICKHOUSE_DATABASE, table=settings.CLICKHOUSE_TABLE
        )
    )
    return client


def connect_rabbitmq(settings: Settings):
    params = pika.URLParameters(settings.RABBITMQ_URL)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    channel.exchange_declare(
        exchange=settings.RABBITMQ_EXCHANGE, exchange_type="direct", durable=True
    )
    channel.queue_declare(queue=settings.RABBITMQ_QUEUE_NAME, durable=True)
    channel.queue_bind(
        exchange=settings.RABBITMQ_EXCHANGE,
        queue=settings.RABBITMQ_QUEUE_NAME,
        routing_key=settings.RABBITMQ_ROUTING_KEY_READ,
    )
    return connection, channel


# ------------------------
# Core Logic
# ------------------------


def insert_flattened_games(client, settings: Settings):
    total_inserted = 0

    for data in load_json_files(settings.GAMES_FLATTEN_DIR):
        if not isinstance(data, list) or not data:
            continue

        rows = []
        for row in data:
            try:
                rows.append(
                    (
                        str(uuid.uuid4()),
                        int(row["game_id"]),
                        parse(row["begin_at"]),
                        int(row.get("map_id") or 0),
                        int(row.get("league_id") or 0),
                        int(row.get("serie_id") or 0),
                        int(row.get("serie_tier") or 0),
                        int(row.get("tournament_id") or 0),
                        int(row.get("team_id") or 0),
                        int(row.get("team_opponent_id") or 0),
                        int(row.get("player_id") or 0),
                        int(row.get("player_opponent_id") or 0),
                        float(row.get("adr") or 0),
                        float(row.get("kast") or 0),
                        float(row.get("rating") or 0),
                        int(row.get("kills") or 0),
                        int(row.get("deaths") or 0),
                        int(row.get("assists") or 0),
                        int(row.get("headshots") or 0),
                        int(row.get("flash_assists") or 0),
                        int(row.get("first_kills_diff") or 0),
                        int(row.get("k_d_diff") or 0),
                        int(row.get("round") or 0),
                        int(row.get("is_ct") or 0),
                        int(row.get("outcome") or 0),
                        int(row.get("win") or 0),
                    )
                )
            except Exception as e:
                log.warning("Skipping bad row in game %s: %s", row.get("game_id"), e)

        if rows:
            client.insert(
                settings.CLICKHOUSE_TABLE,
                rows,
                column_names=[
                    "game_flatten_uuid",
                    "game_id",
                    "begin_at",
                    "map_id",
                    "league_id",
                    "serie_id",
                    "serie_tier",
                    "tournament_id",
                    "team_id",
                    "team_opponent_id",
                    "player_id",
                    "player_opponent_id",
                    "adr",
                    "kast",
                    "rating",
                    "kills",
                    "deaths",
                    "assists",
                    "headshots",
                    "flash_assists",
                    "first_kills_diff",
                    "k_d_diff",
                    "round",
                    "is_ct",
                    "outcome",
                    "win",
                ],
            )
            total_inserted += len(rows)

    log.info("🎯 Total %d records inserted into ClickHouse.", total_inserted)
    return total_inserted


def publish_event(channel, settings: Settings):
    event = {
        "event_uuid": str(uuid.uuid4()),
        "event_type": settings.RABBITMQ_ROUTING_KEY_WRITE,
    }
    channel.basic_publish(
        exchange=settings.RABBITMQ_EXCHANGE,
        routing_key=settings.RABBITMQ_ROUTING_KEY_WRITE,
        body=json.dumps(event).encode("utf-8"),
        properties=pika.BasicProperties(delivery_mode=2),
    )
    log.info("📤 Published event → %s : %s", settings.RABBITMQ_ROUTING_KEY_WRITE, event)


def start_consumer(settings: Settings, client, channel):
    def callback(ch, method, properties, body):
        try:
            log.info("📩 Received event → %s", settings.RABBITMQ_ROUTING_KEY_READ)
            inserted = insert_flattened_games(client, settings)
            log.info("✅ %d rows inserted into ClickHouse", inserted)
            publish_event(channel, settings)
        except Exception as e:
            log.exception("Error while processing message: %s", e)
        finally:
            ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(
        queue=settings.RABBITMQ_QUEUE_NAME, on_message_callback=callback
    )

    log.info(
        "🐇 Waiting for messages with routing key: %s",
        settings.RABBITMQ_ROUTING_KEY_READ,
    )
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        log.info("🛑 Stopped by user.")


# ------------------------
# Entry Point
# ------------------------


def main():
    args = parse_args()
    settings = parse_env_file(args.env_file)
    configure_logger(settings)

    client = connect_clickhouse(settings)
    connection, channel = connect_rabbitmq(settings)

    try:
        start_consumer(settings, client, channel)
    finally:
        if not connection.is_closed:
            connection.close()
        log.info("🐇 RabbitMQ connection closed.")


if __name__ == "__main__":
    main()
