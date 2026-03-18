import json
import os
from pathlib import Path

from fastapi import FastAPI, HTTPException
from minio import Minio
from minio.error import S3Error
import psycopg
from psycopg.rows import dict_row


app = FastAPI(title="Query API")


def get_postgres_connection() -> psycopg.Connection:
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "psychology"),
        user=os.getenv("POSTGRES_USER", "psychology"),
        password=os.getenv("POSTGRES_PASSWORD", "psychology"),
        autocommit=True,
        row_factory=dict_row,
    )


def get_minio_client() -> Minio:
    return Minio(
        endpoint=os.getenv("MINIO_ENDPOINT", "minio:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
    )


def get_analysis_rows() -> list[dict]:
    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    transcript_object_name,
                    transcript_id,
                    source_object_name,
                    audio_object_name,
                    analysis_object_name,
                    dominant_speaker,
                    utterance_count,
                    speaker_count,
                    created_at,
                    updated_at
                FROM analysis_results
                ORDER BY updated_at DESC
                """
            )
            return cursor.fetchall()


def get_analysis_row(transcript_id: str) -> dict | None:
    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    transcript_object_name,
                    transcript_id,
                    source_object_name,
                    audio_object_name,
                    analysis_object_name,
                    dominant_speaker,
                    utterance_count,
                    speaker_count,
                    created_at,
                    updated_at
                FROM analysis_results
                WHERE transcript_id = %s
                """,
                (transcript_id,),
            )
            return cursor.fetchone()


def load_analysis_json(bucket_name: str, analysis_object_name: str) -> dict:
    client = get_minio_client()
    response = client.get_object(bucket_name, analysis_object_name)
    try:
        return json.loads(response.read().decode("utf-8"))
    finally:
        response.close()
        response.release_conn()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/videos")
def list_videos() -> list[dict]:
    rows = get_analysis_rows()
    results = []
    for row in rows:
        results.append(
            {
                "transcript_id": row["transcript_id"],
                "filename": Path(row["source_object_name"]).name,
                "source_object_name": row["source_object_name"],
                "audio_object_name": row["audio_object_name"],
                "analysis_object_name": row["analysis_object_name"],
                "dominant_speaker": row["dominant_speaker"],
                "utterance_count": row["utterance_count"],
                "speaker_count": row["speaker_count"],
                "created_at": row["created_at"].isoformat(),
                "updated_at": row["updated_at"].isoformat(),
            }
        )
    return results


@app.get("/videos/{transcript_id}")
def get_video_analysis(transcript_id: str) -> dict:
    row = get_analysis_row(transcript_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Analysis not found")

    try:
        analysis = load_analysis_json("videos", row["analysis_object_name"])
    except S3Error as exc:
        raise HTTPException(status_code=500, detail=f"MinIO error: {exc}") from exc

    return {
        "transcript_id": row["transcript_id"],
        "filename": Path(row["source_object_name"]).name,
        "source_object_name": row["source_object_name"],
        "audio_object_name": row["audio_object_name"],
        "analysis_object_name": row["analysis_object_name"],
        "dominant_speaker": row["dominant_speaker"],
        "utterance_count": row["utterance_count"],
        "speaker_count": row["speaker_count"],
        "created_at": row["created_at"].isoformat(),
        "updated_at": row["updated_at"].isoformat(),
        "analysis": analysis,
    }
