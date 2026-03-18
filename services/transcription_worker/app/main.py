import json
import logging
import os
import time
from pathlib import Path

import httpx
from minio import Minio
from minio.error import S3Error
import pika


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [transcription_worker] %(message)s",
)
logger = logging.getLogger(__name__)


class PermanentTranscriptionError(Exception):
    pass


class TemporaryTranscriptionError(Exception):
    pass


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


def download_audio(bucket_name: str, audio_object_name: str) -> str:
    download_dir = Path(os.getenv("TRANSCRIPTION_WORKER_DOWNLOAD_DIR", "/tmp/transcription_worker"))
    download_dir.mkdir(parents=True, exist_ok=True)
    local_path = download_dir / Path(audio_object_name).name
    client = get_minio_client()
    client.fget_object(bucket_name, audio_object_name, str(local_path))
    return str(local_path)


def get_assemblyai_api_key() -> str:
    api_key = os.getenv("ASSEMBLYAI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("ASSEMBLYAI_API_KEY is not set")
    return api_key


def get_assemblyai_headers() -> dict[str, str]:
    return {"authorization": get_assemblyai_api_key()}


def upload_audio_to_assemblyai(local_path: str) -> str:
    with open(local_path, "rb") as audio_file:
        response = httpx.post(
            "https://api.assemblyai.com/v2/upload",
            headers=get_assemblyai_headers(),
            content=audio_file,
            timeout=120.0,
        )

    if response.status_code != 200:
        message = response.text
        if 400 <= response.status_code < 500:
            raise PermanentTranscriptionError(f"AssemblyAI upload failed: {message}")
        raise TemporaryTranscriptionError(f"AssemblyAI upload failed: {message}")

    return response.json()["upload_url"]


def request_transcript(upload_url: str) -> str:
    payload = {
        "audio_url": upload_url,
        "speaker_labels": True,
        "language_detection": True,
        "speech_models": ["universal-3-pro", "universal-2"],
    }
    response = httpx.post(
        "https://api.assemblyai.com/v2/transcript",
        headers=get_assemblyai_headers(),
        json=payload,
        timeout=120.0,
    )
    if response.status_code != 200:
        message = response.text
        if 400 <= response.status_code < 500:
            raise PermanentTranscriptionError(f"AssemblyAI transcript request failed: {message}")
        raise TemporaryTranscriptionError(f"AssemblyAI transcript request failed: {message}")
    return response.json()["id"]


def wait_for_transcript_completion(transcript_id: str) -> dict:
    headers = get_assemblyai_headers()
    while True:
        response = httpx.get(
            f"https://api.assemblyai.com/v2/transcript/{transcript_id}",
            headers=headers,
            timeout=120.0,
        )
        if response.status_code != 200:
            message = response.text
            if 400 <= response.status_code < 500:
                raise PermanentTranscriptionError(f"AssemblyAI transcript polling failed: {message}")
            raise TemporaryTranscriptionError(f"AssemblyAI transcript polling failed: {message}")

        transcript = response.json()
        status = transcript.get("status")
        if status == "completed":
            return transcript
        if status == "error":
            raise PermanentTranscriptionError(
                f"AssemblyAI transcription failed: {transcript.get('error', 'unknown error')}"
            )

        time.sleep(3)


def transcribe_audio(local_path: str) -> dict:
    upload_url = upload_audio_to_assemblyai(local_path)
    transcript_id = request_transcript(upload_url)
    return wait_for_transcript_completion(transcript_id)


def log_transcript_preview(transcript: dict) -> None:
    utterances = transcript.get("utterances") or []
    logger.info("Transcript completed. Utterances count: %s", len(utterances))
    if transcript.get("text"):
        logger.info("Transcript preview: %s", transcript["text"][:300])
    for utterance in utterances[:5]:
        logger.info("Speaker %s: %s", utterance.get("speaker"), utterance.get("text"))


def main() -> None:
    queue_name = os.getenv("AUDIO_EXTRACTED_QUEUE", "audio_extracted")

    while True:
        try:
            get_assemblyai_api_key()
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
                logger.info("Received audio extracted event: %s", payload)
                try:
                    local_path = download_audio(
                        bucket_name=payload["bucket"],
                        audio_object_name=payload["audio_object_name"],
                    )
                    logger.info("Downloaded extracted audio to %s", local_path)
                    transcript = transcribe_audio(local_path)
                    log_transcript_preview(transcript)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except PermanentTranscriptionError as exc:
                    logger.error("Permanent transcription failure: %s", exc)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except (TemporaryTranscriptionError, S3Error, KeyError, OSError, httpx.HTTPError) as exc:
                    logger.error("Temporary processing failure: %s", exc)
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

            channel.basic_consume(queue=queue_name, on_message_callback=callback)
            channel.start_consuming()
        except (pika.exceptions.AMQPError, RuntimeError) as exc:
            logger.warning("Transcription worker not ready yet: %s. Retrying in 5 seconds.", exc)
            time.sleep(5)


if __name__ == "__main__":
    main()
