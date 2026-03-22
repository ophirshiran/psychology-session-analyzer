import json
import logging
import os
import time
from collections import Counter
from io import BytesIO
from pathlib import Path
from typing import TypedDict

from google import genai
from google.genai import errors, types
from minio import Minio
from minio.error import S3Error
import pika
import psycopg
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

POSITIVE_EMOTIONS = {"calm", "comfortable", "hopeful", "positive", "relieved", "safe", "supported"}
NEGATIVE_EMOTIONS = {"anxious", "awkward", "conflicted", "embarrassed", "frustrated", "sad", "stressed", "uncomfortable", "upset", "worried"}


class GeminiAnalysisError(Exception):
    pass


class TemporaryGeminiAnalysisError(Exception):
    pass


class SpeakerRoles(TypedDict):
    therapist_speaker: str
    patient_speaker: str
    confidence: str
    reasoning: str


class UtteranceTag(TypedDict):
    utterance_index: int
    speaker: str
    speaker_role: str
    text: str
    topic: str
    emotion: str


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


def get_gemini_api_key() -> str:
    api_key = os.getenv("GEMINI_API_KEY", "").strip()
    if not api_key:
        raise GeminiAnalysisError("GEMINI_API_KEY is not set")
    return api_key


def get_gemini_client() -> genai.Client:
    return genai.Client(api_key=get_gemini_api_key())


def get_postgres_connection() -> psycopg.Connection:
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "psychology"),
        user=os.getenv("POSTGRES_USER", "psychology"),
        password=os.getenv("POSTGRES_PASSWORD", "psychology"),
        autocommit=True,
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


def get_llm_roles_cache_key(transcript_object_name: str) -> str:
    return f"llm_roles:{transcript_object_name}"


def get_llm_tags_cache_key(transcript_object_name: str) -> str:
    return f"llm_tags:v2:{transcript_object_name}"


def load_cached_analysis(transcript_object_name: str) -> dict | None:
    cached_value = get_redis_client().get(get_analysis_cache_key(transcript_object_name))
    if not cached_value:
        return None
    return json.loads(cached_value)


def save_cached_analysis(transcript_object_name: str, analysis: dict) -> None:
    get_redis_client().set(get_analysis_cache_key(transcript_object_name), json.dumps(analysis))


def load_cached_llm_roles(transcript_object_name: str) -> dict | None:
    cached_value = get_redis_client().get(get_llm_roles_cache_key(transcript_object_name))
    if not cached_value:
        return None
    return json.loads(cached_value)


def save_cached_llm_roles(transcript_object_name: str, roles: dict) -> None:
    get_redis_client().set(get_llm_roles_cache_key(transcript_object_name), json.dumps(roles))


def load_cached_llm_tags(transcript_object_name: str) -> list[dict] | None:
    cached_value = get_redis_client().get(get_llm_tags_cache_key(transcript_object_name))
    if not cached_value:
        return None
    return json.loads(cached_value)


def save_cached_llm_tags(transcript_object_name: str, tags: list[dict]) -> None:
    get_redis_client().set(get_llm_tags_cache_key(transcript_object_name), json.dumps(tags))


def ensure_analysis_table_exists() -> None:
    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS analysis_results (
                    transcript_object_name TEXT PRIMARY KEY,
                    transcript_id TEXT,
                    source_object_name TEXT NOT NULL,
                    audio_object_name TEXT NOT NULL,
                    analysis_object_name TEXT NOT NULL,
                    dominant_speaker TEXT,
                    utterance_count INTEGER NOT NULL,
                    speaker_count INTEGER NOT NULL,
                    analysis_json JSONB NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )


def save_analysis_to_postgres(payload: dict, analysis_object_name: str, analysis: dict) -> None:
    summary = analysis["summary"]
    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO analysis_results (
                    transcript_object_name,
                    transcript_id,
                    source_object_name,
                    audio_object_name,
                    analysis_object_name,
                    dominant_speaker,
                    utterance_count,
                    speaker_count,
                    analysis_json
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (transcript_object_name)
                DO UPDATE SET
                    transcript_id = EXCLUDED.transcript_id,
                    source_object_name = EXCLUDED.source_object_name,
                    audio_object_name = EXCLUDED.audio_object_name,
                    analysis_object_name = EXCLUDED.analysis_object_name,
                    dominant_speaker = EXCLUDED.dominant_speaker,
                    utterance_count = EXCLUDED.utterance_count,
                    speaker_count = EXCLUDED.speaker_count,
                    analysis_json = EXCLUDED.analysis_json,
                    updated_at = NOW()
                """,
                (
                    payload["transcript_object_name"],
                    payload.get("transcript_id"),
                    payload["source_object_name"],
                    payload["audio_object_name"],
                    analysis_object_name,
                    summary.get("dominant_speaker"),
                    summary["utterance_count"],
                    summary["speaker_count"],
                    json.dumps(analysis),
                ),
            )


def build_llm_role_prompt(transcript: dict) -> str:
    utterances = transcript.get("utterances") or []
    sample_lines = []
    for utterance in utterances[:12]:
        sample_lines.append(f"Speaker {utterance.get('speaker')}: {utterance.get('text', '')}")
    joined_lines = "\n".join(sample_lines)
    return (
        "You are analyzing a psychology or therapy session transcript.\n"
        "Decide which diarized speaker label is the therapist and which is the patient.\n"
        "Return only valid JSON with this exact shape:\n"
        '{"therapist_speaker":"A","patient_speaker":"B","confidence":"high","reasoning":"short explanation"}\n'
        "If uncertain, still provide your best guess.\n"
        "Transcript sample:\n"
        f"{joined_lines}"
    )


def identify_roles_with_gemini(transcript_object_name: str, transcript: dict) -> dict:
    cached_roles = load_cached_llm_roles(transcript_object_name)
    if cached_roles is not None:
        logger.info("Using cached Gemini role assignment for %s", transcript_object_name)
        return cached_roles

    client = get_gemini_client()
    models_to_try = ["gemini-2.5-flash-lite", "gemini-2.5-flash"]
    last_error: Exception | None = None

    for model_name in models_to_try:
        try:
            response = client.models.generate_content(
                model=model_name,
                contents=build_llm_role_prompt(transcript),
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    response_schema=SpeakerRoles,
                ),
            )
            if getattr(response, "parsed", None):
                roles = dict(response.parsed)
            elif response.text:
                try:
                    roles = json.loads(response.text)
                except json.JSONDecodeError as exc:
                    raise GeminiAnalysisError(f"Gemini returned invalid JSON: {response.text}") from exc
            else:
                raise GeminiAnalysisError("Gemini returned an empty response")

            save_cached_llm_roles(transcript_object_name, roles)
            logger.info("Built and cached Gemini role assignment for %s using %s", transcript_object_name, model_name)
            return roles
        except errors.APIError as exc:
            last_error = exc
            logger.warning("Gemini API error with model %s: %s", model_name, exc)
            if getattr(exc, "status_code", None) and int(exc.status_code) >= 500:
                continue
            raise GeminiAnalysisError(f"Gemini API error: {exc}") from exc
        except Exception as exc:
            last_error = exc
            raise GeminiAnalysisError(f"Gemini role assignment failed: {exc}") from exc
    raise TemporaryGeminiAnalysisError(f"Gemini models unavailable: {last_error}")


def build_llm_tag_prompt(batch_utterances: list[tuple[int, dict]], roles: dict) -> str:
    sample_lines = []
    for utterance_index, utterance in batch_utterances:
        sample_lines.append(
            f"{utterance_index} | Speaker {utterance.get('speaker')} | {utterance.get('text', '')}"
        )
    joined_lines = "\n".join(sample_lines)
    return (
        "You are analyzing a psychology or therapy session transcript.\n"
        "Tag every transcript line below.\n"
        f"Speaker {roles.get('therapist_speaker')} is the therapist.\n"
        f"Speaker {roles.get('patient_speaker')} is the patient.\n"
        "Return only valid JSON as an array. Each item must have exactly these keys:\n"
        '[{"utterance_index":0,"speaker_role":"therapist","topic":"short topic","emotion":"single emotion word"}]\n'
        "Keep topic short, 1 to 3 words. Keep emotion to one word when possible.\n"
        "Use the utterance_index values exactly as provided.\n"
        "Transcript sample:\n"
        f"{joined_lines}"
    )


def validate_llm_tags(raw_tags: object, utterance_lookup: dict[int, dict]) -> list[dict]:
    if not isinstance(raw_tags, list):
        raise GeminiAnalysisError("Gemini tags response is not a JSON array")

    validated_tags = []
    for item in raw_tags:
        if not isinstance(item, dict):
            raise GeminiAnalysisError(f"Gemini tag item is not an object: {item}")
        utterance_index = item.get("utterance_index")
        if not isinstance(utterance_index, int):
            raise GeminiAnalysisError(f"Gemini tag item has invalid utterance_index: {item}")
        if utterance_index not in utterance_lookup:
            raise GeminiAnalysisError(f"Gemini tag item references unknown utterance_index: {item}")
        source_utterance = utterance_lookup[utterance_index]
        validated_item: UtteranceTag = {
            "utterance_index": utterance_index,
            "speaker": str(source_utterance.get("speaker", "")).strip(),
            "speaker_role": str(item.get("speaker_role", "")).strip(),
            "text": str(source_utterance.get("text", "")).strip(),
            "topic": str(item.get("topic", "")).strip(),
            "emotion": str(item.get("emotion", "")).strip(),
        }
        if (
            not validated_item["speaker"]
            or not validated_item["speaker_role"]
            or not validated_item["text"]
            or not validated_item["topic"]
            or not validated_item["emotion"]
        ):
            raise GeminiAnalysisError(f"Gemini tag item is missing required fields: {item}")
        validated_tags.append(dict(validated_item))
    return validated_tags


def tag_utterances_with_gemini(transcript_object_name: str, transcript: dict, roles: dict) -> list[dict]:
    cached_tags = load_cached_llm_tags(transcript_object_name)
    if cached_tags is not None:
        logger.info("Using cached Gemini utterance tags for %s", transcript_object_name)
        return cached_tags

    utterances = transcript.get("utterances") or []
    if not utterances:
        return []

    client = get_gemini_client()
    models_to_try = ["gemini-2.5-flash-lite", "gemini-2.5-flash"]
    batch_size = int(os.getenv("GEMINI_TAG_BATCH_SIZE", "12"))
    all_tags: list[dict] = []

    for start in range(0, len(utterances), batch_size):
        batch_utterances = list(enumerate(utterances[start:start + batch_size], start=start))
        utterance_lookup = {index: utterance for index, utterance in batch_utterances}
        last_error: Exception | None = None

        for model_name in models_to_try:
            try:
                response = client.models.generate_content(
                    model=model_name,
                    contents=build_llm_tag_prompt(batch_utterances, roles),
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json",
                    ),
                )
                if not response.text:
                    raise GeminiAnalysisError("Gemini returned an empty tags response")
                try:
                    batch_tags = validate_llm_tags(json.loads(response.text), utterance_lookup)
                except json.JSONDecodeError as exc:
                    raise GeminiAnalysisError(f"Gemini returned invalid tags JSON: {response.text}") from exc

                all_tags.extend(sorted(batch_tags, key=lambda tag: tag["utterance_index"]))
                logger.info(
                    "Tagged utterance batch %s-%s for %s using %s",
                    start,
                    start + len(batch_utterances) - 1,
                    transcript_object_name,
                    model_name,
                )
                break
            except errors.APIError as exc:
                last_error = exc
                logger.warning("Gemini API error while tagging with model %s: %s", model_name, exc)
                if getattr(exc, "status_code", None) and int(exc.status_code) >= 500:
                    continue
                raise GeminiAnalysisError(f"Gemini API error while tagging: {exc}") from exc
            except Exception as exc:
                last_error = exc
                raise GeminiAnalysisError(f"Gemini utterance tagging failed: {exc}") from exc
        else:
            raise TemporaryGeminiAnalysisError(f"Gemini tagging models unavailable: {last_error}")

    save_cached_llm_tags(transcript_object_name, all_tags)
    logger.info("Built and cached Gemini utterance tags for %s", transcript_object_name)
    return all_tags


def build_recommendations(utterance_tags: list[dict]) -> dict:
    patient_tags = [tag for tag in utterance_tags if tag.get("speaker_role") == "patient"]
    positive_topics = []
    negative_topics = []

    for tag in patient_tags:
        topic = tag.get("topic", "")
        emotion = tag.get("emotion", "").lower()
        if not topic:
            continue
        topic_entry = {
            "topic": topic,
            "emotion": tag.get("emotion", ""),
            "text": tag.get("text", ""),
        }
        if emotion in NEGATIVE_EMOTIONS:
            negative_topics.append(topic_entry)
        elif emotion in POSITIVE_EMOTIONS:
            positive_topics.append(topic_entry)

    recommendations = []
    if negative_topics:
        recommendations.append(
            "Explore the patient's negative topics in more depth and ask follow-up questions about what triggers them."
        )
    if positive_topics:
        recommendations.append(
            "Revisit the patient's positive topics and ask what helps those moments feel safer or more manageable."
        )
    if not recommendations:
        recommendations.append(
            "Continue gathering more detail from the patient before making strong topic-based recommendations."
        )

    return {
        "positive_patient_topics": positive_topics[:3],
        "negative_patient_topics": negative_topics[:3],
        "therapist_recommendations": recommendations,
    }


def main() -> None:
    queue_name = os.getenv("TRANSCRIPT_CREATED_QUEUE", "transcript_created")

    while True:
        try:
            ensure_analysis_table_exists()
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
                    roles = identify_roles_with_gemini(payload["transcript_object_name"], transcript)
                    analysis["speaker_roles"] = roles
                    logger.info("Gemini role assignment: %s", roles)
                    utterance_tags = tag_utterances_with_gemini(
                        payload["transcript_object_name"],
                        transcript,
                        roles,
                    )
                    analysis["utterance_tags"] = utterance_tags
                    analysis["sample_tags"] = utterance_tags[:8]
                    logger.info("Gemini utterance tags count: %s", len(utterance_tags))
                    if utterance_tags:
                        logger.info("Gemini first utterance tag: %s", utterance_tags[0])
                    analysis["recommendations"] = build_recommendations(utterance_tags)
                    logger.info("Therapist recommendations: %s", analysis["recommendations"]["therapist_recommendations"])
                    analysis_object_name = upload_analysis(
                        bucket_name=payload["bucket"],
                        transcript_object_name=payload["transcript_object_name"],
                        analysis=analysis,
                    )
                    logger.info("Uploaded analysis JSON to %s", analysis_object_name)
                    save_analysis_to_postgres(payload, analysis_object_name, analysis)
                    logger.info("Saved analysis metadata to Postgres for %s", payload["transcript_object_name"])
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except (
                    S3Error,
                    KeyError,
                    json.JSONDecodeError,
                    GeminiAnalysisError,
                    OSError,
                    TemporaryGeminiAnalysisError,
                    psycopg.Error,
                    redis.RedisError,
                ) as exc:
                    logger.error("Failed to process transcript JSON: %s", exc)
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

            channel.basic_consume(queue=queue_name, on_message_callback=callback)
            channel.start_consuming()
        except (pika.exceptions.AMQPError, psycopg.Error) as exc:
            logger.warning("Analysis worker not ready yet: %s. Retrying in 5 seconds.", exc)
            time.sleep(5)


if __name__ == "__main__":
    main()
