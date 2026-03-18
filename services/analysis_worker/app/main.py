import json
import logging
import os
import time
from collections import Counter
from io import BytesIO
from pathlib import Path

from minio import Minio
from minio.error import S3Error
import pika
import redis


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [analysis_worker] %(message)s",
)
logger = logging.getLogger(__name__)

STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "but",
    "by",
    "for",
    "from",
    "i",
    "in",
    "is",
    "it",
    "of",
    "on",
    "or",
    "so",
    "that",
    "the",
    "their",
    "there",
    "they",
    "this",
    "to",
    "um",
    "was",
    "we",
    "with",
    "you",
    "your",
}


def get_minio_client() -> Minio:
    return Minio(
        endpoint=os.getenv("MINIO_ENDPOINT", "minio:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
    )


def get_redis_client() -> redis.Redis:
    return redis.Redis(
        host=os.getenv("REDIS_HOST", "redis"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        decode_responses=True,
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


def build_basic_analysis(transcript: dict) -> dict:
    utterances = transcript.get("utterances") or []
    speaker_counts: Counter[str] = Counter()
    speaker_word_counts: Counter[str] = Counter()
    keyword_counts: Counter[str] = Counter()

    for utterance in utterances:
        speaker = str(utterance.get("speaker", "unknown"))
        text = utterance.get("text", "")
        speaker_counts[speaker] += 1
        words = [
            word.strip(".,!?;:\"'()[]{}").lower()
            for word in text.split()
        ]
        cleaned_words = [word for word in words if len(word) > 2 and word not in STOPWORDS]
        speaker_word_counts[speaker] += len(cleaned_words)
        keyword_counts.update(cleaned_words)

    return {
        "summary": {
            "utterance_count": len(utterances),
            "speaker_count": len(speaker_counts),
            "dominant_speaker": speaker_counts.most_common(1)[0][0] if speaker_counts else None,
        },
        "speaker_stats": [
            {
                "speaker": speaker,
                "utterances": count,
                "words": speaker_word_counts[speaker],
            }
            for speaker, count in speaker_counts.most_common()
        ],
        "top_keywords": [
            {"word": word, "count": count}
            for word, count in keyword_counts.most_common(10)
        ],
    }


def upload_analysis(bucket_name: str, transcript_object_name: str, analysis: dict) -> str:
    analysis_object_name = f"analysis/{Path(transcript_object_name).stem}.json"
    analysis_bytes = json.dumps(analysis, ensure_ascii=True, indent=2).encode("utf-8")
    client = get_minio_client()
    client.put_object(
        bucket_name=bucket_name,
        object_name=analysis_object_name,
        data=BytesIO(analysis_bytes),
        length=len(analysis_bytes),
        content_type="application/json",
    )
    return analysis_object_name


def get_analysis_cache_key(transcript_object_name: str) -> str:
    return f"analysis:{transcript_object_name}"


def load_cached_analysis(transcript_object_name: str) -> dict | None:
    cached_value = get_redis_client().get(get_analysis_cache_key(transcript_object_name))
    if not cached_value:
        return None
    return json.loads(cached_value)


def save_cached_analysis(transcript_object_name: str, analysis: dict) -> None:
    get_redis_client().set(get_analysis_cache_key(transcript_object_name), json.dumps(analysis))


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
                    analysis = load_cached_analysis(payload["transcript_object_name"])
                    if analysis is not None:
                        logger.info("Using cached analysis for %s", payload["transcript_object_name"])
                    else:
                        analysis = build_basic_analysis(transcript)
                        save_cached_analysis(payload["transcript_object_name"], analysis)
                        logger.info("Built and cached basic analysis: %s", analysis["summary"])
                    analysis_object_name = upload_analysis(
                        bucket_name=payload["bucket"],
                        transcript_object_name=payload["transcript_object_name"],
                        analysis=analysis,
                    )
                    logger.info("Uploaded analysis JSON to %s", analysis_object_name)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except (S3Error, KeyError, json.JSONDecodeError, OSError, redis.RedisError) as exc:
                    logger.error("Failed to process transcript JSON: %s", exc)
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

            channel.basic_consume(queue=queue_name, on_message_callback=callback)
            channel.start_consuming()
        except pika.exceptions.AMQPError as exc:
            logger.warning("RabbitMQ not ready yet: %s. Retrying in 5 seconds.", exc)
            time.sleep(5)


if __name__ == "__main__":
    main()
