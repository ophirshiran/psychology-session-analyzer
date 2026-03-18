import json
import logging
import os
import time

import pika


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [analysis_worker] %(message)s",
)
logger = logging.getLogger(__name__)


def get_connection() -> pika.BlockingConnection:
    credentials = pika.PlainCredentials(
        os.getenv("RABBITMQ_USER", "guest"),
        os.getenv("RABBITMQ_PASSWORD", "guest"),
    )
    parameters = pika.ConnectionParameters(
        host=os.getenv("RABBITMQ_HOST", "rabbitmq"),
        port=int(os.getenv("RABBITMQ_PORT", "5672")),
        credentials=credentials,
    )
    return pika.BlockingConnection(parameters)


def main() -> None:
    queue_name = os.getenv("TRANSCRIPT_CREATED_QUEUE", "transcript_created")

    while True:
        try:
            connection = get_connection()
            channel = connection.channel()
            channel.queue_declare(queue=queue_name, durable=True)
            channel.basic_qos(prefetch_count=1)
            logger.info("Connected to RabbitMQ. Waiting for messages in queue '%s'.", queue_name)

            def callback(
                ch: pika.adapters.blocking_connection.BlockingChannel,
                method: pika.spec.Basic.Deliver,
                properties: pika.spec.BasicProperties,
                body: bytes,
            ) -> None:
                payload = json.loads(body.decode("utf-8"))
                logger.info("Received transcript created event: %s", payload)
                ch.basic_ack(delivery_tag=method.delivery_tag)

            channel.basic_consume(queue=queue_name, on_message_callback=callback)
            channel.start_consuming()
        except pika.exceptions.AMQPError as exc:
            logger.warning("RabbitMQ not ready yet: %s. Retrying in 5 seconds.", exc)
            time.sleep(5)


if __name__ == "__main__":
    main()
