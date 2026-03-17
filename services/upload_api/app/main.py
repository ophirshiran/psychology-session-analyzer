import os
from pathlib import Path
from uuid import uuid4

from fastapi import FastAPI, File, HTTPException, UploadFile
from minio import Minio
from minio.error import S3Error


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

    return {
        "message": "upload succeeded",
        "bucket": bucket_name,
        "object_name": object_name,
        "filename": safe_filename,
    }
