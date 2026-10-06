import logging
import os

from common.contracts import ValidationError, parse_json, validate_score
from common.kafka import (
    configure_logging,
    consume_loop,
    consumer_config,
    producer_config,
    reject_message,
    stop_event,
)

logger = logging.getLogger(__name__)

INSERT_SCORE = """
    INSERT INTO scores (transaction_id, score, fraud_flag, model_version)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (transaction_id) DO NOTHING
"""


def insert_score(connection, score: dict) -> bool:
    """First insert wins; replays preserve the score and arrival order."""
    try:
        with connection.cursor() as cursor:
            cursor.execute(INSERT_SCORE, (score["transaction_id"], score["score"], score["fraud_flag"], score["model_version"]))
            inserted = cursor.rowcount == 1
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    return inserted


def process_message(message, consumer, producer, connection, dlq_topic="dead_letters") -> None:
    try:
        score = validate_score(parse_json(message.value()))
    except ValidationError as error:
        reject_message(consumer, producer, message, error, dlq_topic)
        return

    inserted = insert_score(connection, score)
    # Commit Kafka after PostgreSQL; the primary key handles replays after a crash.
    consumer.commit(message=message, asynchronous=False)
    logger.info("%s transaction %s", "Stored" if inserted else "Deduplicated", score["transaction_id"])


def main() -> None:
    import psycopg2
    from confluent_kafka import Consumer, Producer

    configure_logging()
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    scores_topic = os.getenv("SCORES_TOPIC", "scores")
    dlq_topic = os.getenv("DLQ_TOPIC", "dead_letters")
    stop = stop_event()
    connection = psycopg2.connect(connect_timeout=10)
    consumer = Consumer(consumer_config(bootstrap, "scoring-writer-v1"))
    producer = Producer(producer_config(bootstrap))
    try:
        consumer.subscribe([scores_topic])
        logger.info("Reading %s and storing acknowledged scores", scores_topic)
        consume_loop(
            consumer,
            lambda message: process_message(message, consumer, producer, connection, dlq_topic),
            stop,
        )
    finally:
        consumer.close()
        producer.flush(5.0)
        connection.close()


if __name__ == "__main__":
    main()
