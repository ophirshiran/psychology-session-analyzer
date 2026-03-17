import json
import logging
import os
import subprocess
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


def extract_mp3(video_path: str, source_object_name: str) -> tuple[str, str]:
    output_dir = Path(os.getenv("AUDIO_WORKER_OUTPUT_DIR", "/tmp/audio_worker_output"))
    output_dir.mkdir(parents=True, exist_ok=True)

    audio_filename = f"{Path(source_object_name).stem}.mp3"
    audio_path = output_dir / audio_filename
    audio_object_name = f"audio/{audio_filename}"

    subprocess.run(
        [
            "ffmpeg",
            "-y",
            "-i",
            video_path,
            "-vn",
            "-acodec",
            "libmp3lame",
            str(audio_path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    return str(audio_path), audio_object_name


def upload_mp3(bucket_name: str, object_name: str, audio_path: str) -> None:
    client = get_minio_client()
    client.fput_object(
        bucket_name=bucket_name,
        object_name=object_name,
        file_path=audio_path,
        content_type="audio/mpeg",
    )


def publish_audio_extracted_event(
    channel: pika.adapters.blocking_connection.BlockingChannel,
    bucket_name: str,
    source_object_name: str,
    audio_object_name: str,
    filename: str,
) -> None:
    queue_name = os.getenv("AUDIO_EXTRACTED_QUEUE", "audio_extracted")
    payload = {
        "event_type": "audio.extracted",
        "bucket": bucket_name,
        "source_object_name": source_object_name,
        "audio_object_name": audio_object_name,
        "filename": filename,
    }
    channel.queue_declare(queue=queue_name, durable=True)
    channel.basic_publish(
        exchange="",
        routing_key=queue_name,
        body=json.dumps(payload),
        properties=pika.BasicProperties(delivery_mode=2),
    )


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
                    audio_path, audio_object_name = extract_mp3(
                        video_path=local_path,
                        source_object_name=payload["object_name"],
                    )
                    logger.info("Extracted mp3 to %s", audio_path)
                    upload_mp3(
                        bucket_name=payload["bucket"],
                        object_name=audio_object_name,
                        audio_path=audio_path,
                    )
                    logger.info(
                        "Uploaded extracted mp3 to bucket '%s' as '%s'",
                        payload["bucket"],
                        audio_object_name,
                    )
                    publish_audio_extracted_event(
                        channel=ch,
                        bucket_name=payload["bucket"],
                        source_object_name=payload["object_name"],
                        audio_object_name=audio_object_name,
                        filename=payload["filename"],
                    )
                    logger.info("Published audio.extracted event for '%s'", audio_object_name)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except (S3Error, KeyError, subprocess.CalledProcessError) as exc:
                    logger.error("Failed to process source video: %s", exc)
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

            channel.basic_consume(queue=queue_name, on_message_callback=callback)
            channel.start_consuming()
        except pika.exceptions.AMQPError as exc:
            logger.warning("RabbitMQ not ready yet: %s. Retrying in 5 seconds.", exc)
            time.sleep(5)


if __name__ == "__main__":
    main()
