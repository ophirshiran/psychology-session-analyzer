import json
import logging
import os
import time
from pathlib import Path

from minio import Minio
from minio.error import S3Error
import pika


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [analysis_worker] %(message)s",
)
logger = logging.getLogger(__name__)


def get_minio_client() -> Minio:
    return Minio(
        endpoint=os.getenv("MINIO_ENDPOINT", "minio:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
    )


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


def download_transcript(bucket_name: str, transcript_object_name: str) -> tuple[str, dict]:
    download_dir = Path(os.getenv("ANALYSIS_WORKER_DOWNLOAD_DIR", "/tmp/analysis_worker"))
    download_dir.mkdir(parents=True, exist_ok=True)
    local_path = download_dir / Path(transcript_object_name).name
    client = get_minio_client()
    client.fget_object(bucket_name, transcript_object_name, str(local_path))
    transcript = json.loads(local_path.read_text(encoding="utf-8"))
    return str(local_path), transcript


def log_transcript_preview(transcript: dict) -> None:
    utterances = transcript.get("utterances") or []
    logger.info("Transcript JSON loaded. Utterances count: %s", len(utterances))
    if transcript.get("text"):
        logger.info("Transcript JSON preview: %s", transcript["text"][:250])


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
                try:
                    local_path, transcript = download_transcript(
                        bucket_name=payload["bucket"],
                        transcript_object_name=payload["transcript_object_name"],
                    )
                    logger.info("Downloaded transcript JSON to %s", local_path)
                    log_transcript_preview(transcript)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except (S3Error, KeyError, json.JSONDecodeError, OSError) as exc:
                    logger.error("Failed to process transcript JSON: %s", exc)
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

            channel.basic_consume(queue=queue_name, on_message_callback=callback)
            channel.start_consuming()
        except pika.exceptions.AMQPError as exc:
            logger.warning("RabbitMQ not ready yet: %s. Retrying in 5 seconds.", exc)
            time.sleep(5)


if __name__ == "__main__":
    main()
