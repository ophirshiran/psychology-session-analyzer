# Psychology Session Analyzer

## Overview

Psychology Session Analyzer is a Python microservice system for processing recorded therapy sessions end to end.

The platform accepts a session video, stores it, extracts audio, generates a diarized transcript, enriches the transcript with LLM-based analysis, and exposes the final results through an API.

## Features

- Video upload through a FastAPI service
- Object storage for raw videos, extracted audio, transcripts, and analysis artifacts
- Event-driven processing with RabbitMQ
- Audio extraction to MP3
- Transcription with AssemblyAI, including speaker diarization
- Therapist and patient role identification with Gemini
- Topic and emotion tagging for all transcript utterances
- Therapist recommendations based on detected patient themes and emotions
- Redis caching for expensive analysis steps
- Postgres persistence for analysis metadata
- Centralized container log collection with Datadog

## Architecture

### Services

- `upload_api`: accepts video uploads, stores them in MinIO, publishes `video.uploaded`
- `audio_worker`: consumes `video.uploaded`, downloads the video, extracts MP3, uploads it to MinIO, publishes `audio.extracted`
- `transcription_worker`: consumes `audio.extracted`, downloads the MP3, calls AssemblyAI, uploads transcript JSON to MinIO, publishes `transcript.created`
- `analysis_worker`: consumes `transcript.created`, downloads the transcript, runs basic analysis and Gemini enrichment, caches results in Redis, stores metadata in Postgres, uploads final analysis JSON to MinIO
- `query_api`: exposes analysis retrieval endpoints
- `rabbitmq`: internal message broker
- `minio`: object storage
- `redis`: cache for repeated analysis work
- `postgres`: analysis metadata database
- `datadog_agent`: centralized log collection

### Processing Flow

```text
user
  -> upload_api
  -> MinIO (raw video)
  -> RabbitMQ: video.uploaded
  -> audio_worker
  -> MinIO (mp3)
  -> RabbitMQ: audio.extracted
  -> transcription_worker
  -> AssemblyAI
  -> MinIO (transcript JSON)
  -> RabbitMQ: transcript.created
  -> analysis_worker
  -> Redis cache
  -> Postgres metadata
  -> MinIO (analysis JSON)
  -> query_api
```

## Repository Structure

```text
psychology-session-analyzer/
  docker-compose.yml
  .env.example
  services/
    upload_api/
    audio_worker/
    transcription_worker/
    analysis_worker/
    query_api/
```

## Technology Stack

- Python 3.12
- FastAPI
- RabbitMQ
- MinIO
- Redis
- Postgres
- AssemblyAI
- Gemini
- Docker
- Docker Compose
- Datadog Agent

## Environment Variables

Create a local `.env` file from `.env.example`.

```bash
cp .env.example .env
```

Required variables:

```env
ASSEMBLYAI_API_KEY=replace_with_your_assemblyai_api_key
GEMINI_API_KEY=replace_with_your_gemini_api_key
DD_API_KEY=replace_with_your_datadog_api_key
DD_SITE=datadoghq.com
```

Notes:

- `ASSEMBLYAI_API_KEY` is required for transcription.
- `GEMINI_API_KEY` is required for speaker-role detection, utterance tagging, and recommendations.
- `DD_API_KEY` and `DD_SITE` are required for Datadog log collection.
- Do not commit `.env`.

## Running the System

### 1. Start the full stack

```bash
docker compose up -d --build
```

### 2. Verify the main services

- RabbitMQ UI: `http://localhost:15672`
- MinIO UI: `http://localhost:9001`
- Upload API docs: `http://localhost:8000/docs`
- Query API docs: `http://localhost:8001/docs`

Optional health checks:

```bash
curl http://localhost:8000/health
curl http://localhost:8001/health
```

### 3. Upload a therapy session video

```bash
curl -X POST http://localhost:8000/upload \
  -F 'file=@/absolute/path/to/therapy-session.mp4'
```

Expected pipeline result:

- The raw video is stored in MinIO under `videos/raw/`
- An upload event is published to RabbitMQ
- The MP3 file is stored in MinIO under `videos/audio/`
- The transcript JSON is stored in MinIO under `videos/transcripts/`
- The analysis JSON is stored in MinIO under `videos/analysis/`

### 4. Read analysis results

List analyzed sessions:

```bash
curl http://localhost:8001/videos
```

Get one analysis by transcript id:

```bash
curl http://localhost:8001/videos/<transcript_id>
```

## Analysis Output

The analysis output currently includes:

- summary:
  - utterance count
  - speaker count
  - dominant speaker
- speaker statistics
- top keywords
- speaker roles:
  - therapist speaker label
  - patient speaker label
- full utterance tags:
  - utterance index
  - speaker
  - speaker role
  - topic
  - emotion
- sample utterance tags:
  - a short preview subset for quick inspection
- therapist recommendations:
  - positive patient topics
  - negative patient topics
  - short suggestions for future sessions

## Storage Layout in MinIO

The `videos` bucket is used as the main storage bucket.

- `raw/...mp4`: uploaded source videos
- `audio/...mp3`: extracted audio files
- `transcripts/...json`: AssemblyAI transcription results
- `analysis/...json`: final analysis output

## RabbitMQ Queues

- `video_uploaded`
- `audio_extracted`
- `transcript_created`

## Database

Postgres stores analysis metadata in the `analysis_results` table.

Saved data includes:

- transcript object name
- transcript id
- source video object name
- audio object name
- analysis object name
- dominant speaker
- utterance count
- speaker count
- full analysis JSON
- timestamps

Host access:

- Postgres is exposed on local port `5433`

## Logging and Observability

Each service logs to standard output.

The Datadog Agent is configured to:

- authenticate with `DD_API_KEY`
- collect all container logs
- tail Docker container log files

## Implementation Notes

- Internal service-to-service communication is event-driven through RabbitMQ, not REST.
- The only user-facing APIs are `upload_api` and `query_api`.
- LLM-based speaker roles and utterance tags are cached in Redis to avoid repeated Gemini calls.
- The analysis worker also caches the basic transcript analysis.
- Full transcript utterance tagging is processed in Gemini batches to keep requests smaller and more reliable.

## Suggested Demo Flow

For a short product demo, show:

1. `docker compose up -d --build`
2. Upload a therapy session video
3. MinIO showing raw video, MP3, transcript JSON, and analysis JSON
4. RabbitMQ queues receiving and consuming events
5. Query API returning `/videos` and `/videos/{id}`
6. Datadog logs from the containers

## Recommended Sample Input

Use a short English therapy or mock therapy session video with:

- at least two speakers
- clear audio
- little or no background music
- public availability, for example a mock therapy session from YouTube
