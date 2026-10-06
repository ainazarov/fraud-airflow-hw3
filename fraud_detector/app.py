import logging
import os

from common.contracts import ValidationError, parse_json, validate_score, validate_transaction
from common.kafka import (
    configure_logging,
    consume_loop,
    consumer_config,
    producer_config,
    publish_json,
    reject_message,
    stop_event,
)

logger = logging.getLogger(__name__)


def process_message(message, consumer, producer, scorer, scores_topic="scores", dlq_topic="dead_letters") -> None:
    try:
        transaction = validate_transaction(parse_json(message.value()))
    except ValidationError as error:
        reject_message(consumer, producer, message, error, dlq_topic)
        return

    # Model failures leave the input uncommitted for retry after restart.
    prediction = scorer.score(transaction["data"])
    result = validate_score({"transaction_id": transaction["transaction_id"], **prediction})
    publish_json(producer, scores_topic, result, key=result["transaction_id"])
    consumer.commit(message=message, asynchronous=False)
    logger.info("Scored transaction %s: score=%.6f fraud_flag=%s model_version=%s", result["transaction_id"], result["score"], result["fraud_flag"], result["model_version"])


def main() -> None:
    from confluent_kafka import Consumer, Producer

    from fraud_detector.scorer import FraudScorer

    configure_logging()
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    transactions_topic = os.getenv("TRANSACTIONS_TOPIC", "transactions")
    scores_topic = os.getenv("SCORES_TOPIC", "scores")
    dlq_topic = os.getenv("DLQ_TOPIC", "dead_letters")
    stop = stop_event()
    scorer = FraudScorer()
    consumer = Consumer(consumer_config(bootstrap, "fraud-detector-v1"))
    producer = Producer(producer_config(bootstrap))
    try:
        consumer.subscribe([transactions_topic])
        logger.info("Reading %s; publishing %s", transactions_topic, scores_topic)
        consume_loop(
            consumer,
            lambda message: process_message(message, consumer, producer, scorer, scores_topic, dlq_topic),
            stop,
        )
    finally:
        consumer.close()
        producer.flush(5.0)


if __name__ == "__main__":
    main()
