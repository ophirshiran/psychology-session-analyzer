import os
import json
from pathlib import Path
from uuid import uuid4

from fastapi import FastAPI, File, HTTPException, UploadFile
from minio import Minio
from minio.error import S3Error
import pika


app = FastAPI(title="Upload API")


def get_minio_client() -> Minio:
    return Minio(
        endpoint=os.getenv("MINIO_ENDPOINT", "minio:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
    )


def ensure_bucket_exists(client: Minio, bucket_name: str) -> None:
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)


def publish_video_uploaded_event(
    bucket_name: str,
    object_name: str,
    filename: str,
) -> None:
    queue_name = os.getenv("VIDEO_UPLOADED_QUEUE", "video_uploaded")
    credentials = pika.PlainCredentials(
        os.getenv("RABBITMQ_USER", "guest"),
        os.getenv("RABBITMQ_PASSWORD", "guest"),
    )
    parameters = pika.ConnectionParameters(
        host=os.getenv("RABBITMQ_HOST", "rabbitmq"),
        port=int(os.getenv("RABBITMQ_PORT", "5672")),
        credentials=credentials,
    )
    payload = {
        "event_type": "video.uploaded",
        "bucket": bucket_name,
        "object_name": object_name,
        "filename": filename,
    }

    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True)
    channel.basic_publish(
        exchange="",
        routing_key=queue_name,
        body=json.dumps(payload),
        properties=pika.BasicProperties(delivery_mode=2),
    )
    connection.close()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/upload")
def upload_video(file: UploadFile = File(...)) -> dict[str, str]:
    bucket_name = os.getenv("MINIO_BUCKET", "videos")
    safe_filename = Path(file.filename or "uploaded-file.bin").name
    object_name = f"raw/{uuid4()}-{safe_filename}"
    client = get_minio_client()

    try:
        ensure_bucket_exists(client, bucket_name)

        file.file.seek(0, os.SEEK_END)
        file_size = file.file.tell()
        file.file.seek(0)

        client.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=file.file,
            length=file_size,
            content_type=file.content_type or "application/octet-stream",
        )
    except S3Error as exc:
        raise HTTPException(status_code=500, detail=f"MinIO error: {exc}") from exc

    try:
        publish_video_uploaded_event(bucket_name, object_name, safe_filename)
    except pika.exceptions.AMQPError as exc:
        raise HTTPException(status_code=500, detail=f"RabbitMQ error: {exc}") from exc

    return {
        "message": "upload succeeded",
        "bucket": bucket_name,
        "object_name": object_name,
        "filename": safe_filename,
        "event": "video.uploaded",
    }
