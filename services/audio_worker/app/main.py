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
    format="%(asctime)s %(levelname)s [audio_worker] %(message)s",
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


def download_video(bucket_name: str, object_name: str, filename: str) -> str:
    download_dir = Path(os.getenv("AUDIO_WORKER_DOWNLOAD_DIR", "/tmp/audio_worker"))
    download_dir.mkdir(parents=True, exist_ok=True)
    local_path = download_dir / Path(filename).name
    client = get_minio_client()
    client.fget_object(bucket_name, object_name, str(local_path))
    return str(local_path)


def main() -> None:
    queue_name = os.getenv("VIDEO_UPLOADED_QUEUE", "video_uploaded")

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
                logger.info("Received video upload event: %s", payload)
                try:
                    local_path = download_video(
                        bucket_name=payload["bucket"],
                        object_name=payload["object_name"],
                        filename=payload["filename"],
                    )
                    logger.info("Downloaded source video to %s", local_path)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except (S3Error, KeyError) as exc:
                    logger.error("Failed to download source video: %s", exc)
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

            channel.basic_consume(queue=queue_name, on_message_callback=callback)
            channel.start_consuming()
        except pika.exceptions.AMQPError as exc:
            logger.warning("RabbitMQ not ready yet: %s. Retrying in 5 seconds.", exc)
            time.sleep(5)


if __name__ == "__main__":
    main()
