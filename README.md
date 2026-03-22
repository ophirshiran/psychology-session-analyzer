# Psychology Session Analyzer

Assignment 2 - Advanced Systems Development Using AI 2025-2026

## Overview

This project implements a microservice-based system that analyzes recorded psychology or therapy sessions.

The system follows the required assignment flow:

1. A user uploads a therapy session video through a FastAPI service.
2. The video is stored in MinIO.
3. An audio worker extracts an MP3 file from the uploaded video.
4. A transcription worker sends the audio to AssemblyAI and receives a transcript with speaker diarization.
5. An analysis worker processes the transcript, identifies therapist and patient speakers with Gemini, tags all transcript utterances with topics and emotions in batches, generates therapist recommendations, caches expensive LLM results in Redis, and stores analysis metadata in Postgres.
6. A second FastAPI service exposes the saved analysis results through API calls.
7. A Datadog Agent collects logs from all containers.

## Assignment Coverage

This implementation covers the main exercise requirements:

- Multiple Python microservices, each in its own Docker container
- Docker Compose orchestration
- RabbitMQ event-based communication between services
- MinIO object storage for raw videos, extracted audio, transcripts, and analyses
- FastAPI upload API
- FastAPI query API
- Audio extraction to MP3
- AssemblyAI transcription with speaker diarization
- Gemini-based speaker role identification
- Gemini-based topic and emotion tagging
- Redis caching for repeated analysis work
- Postgres persistence for analysis metadata
- Datadog centralized log collection
- Additional analysis feature: therapist recommendations based on detected patient topics and emotions

## Architecture

### Services

- `upload_api`: accepts video uploads, stores them in MinIO, publishes `video.uploaded`
- `audio_worker`: consumes `video.uploaded`, downloads the video, extracts MP3, uploads it to MinIO, publishes `audio.extracted`
- `transcription_worker`: consumes `audio.extracted`, downloads the MP3, calls AssemblyAI, uploads transcript JSON to MinIO, publishes `transcript.created`
- `analysis_worker`: consumes `transcript.created`, downloads the transcript, runs basic analysis plus Gemini analysis, caches results in Redis, stores metadata in Postgres, uploads final analysis JSON to MinIO
- `query_api`: exposes analysis retrieval endpoints for end users
- `rabbitmq`: internal message broker
- `minio`: object storage
- `redis`: cache for expensive analysis work
- `postgres`: analysis metadata database
- `datadog_agent`: centralized log collection

### Event Flow

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

## Main Technologies

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

Required variables:

```env
ASSEMBLYAI_API_KEY=replace_with_your_assemblyai_api_key
GEMINI_API_KEY=replace_with_your_gemini_api_key
DD_API_KEY=replace_with_your_datadog_api_key
DD_SITE=datadoghq.com
```

Notes:

- `ASSEMBLYAI_API_KEY` is required for transcription.
- `GEMINI_API_KEY` is required for therapist/patient role detection and topic/emotion tagging.
- `DD_API_KEY` and `DD_SITE` are required for Datadog log collection.
- Do not commit `.env`.

## How to Run

### 1. Prepare environment

```bash
cp .env.example .env
```

Fill in the real API keys in `.env`.

### 2. Start the full system

```bash
docker compose up -d --build
```

### 3. Verify infrastructure

- RabbitMQ UI: `http://localhost:15672`
- MinIO UI: `http://localhost:9001`
- Upload API docs: `http://localhost:8000/docs`
- Query API docs: `http://localhost:8001/docs`

### 4. Upload a therapy session video

Example:

```bash
curl -X POST http://localhost:8000/upload \
  -F 'file=@/absolute/path/to/therapy-session.mp4'
```

Expected result:

- The raw video is saved in MinIO under `videos/raw/`
- An event is published to RabbitMQ
- The MP3 is saved in MinIO under `videos/audio/`
- The transcript JSON is saved in MinIO under `videos/transcripts/`
- The analysis JSON is saved in MinIO under `videos/analysis/`

### 5. Read analysis results

List analyzed sessions:

```bash
curl http://localhost:8001/videos
```

Get one analysis by transcript id:

```bash
curl http://localhost:8001/videos/<transcript_id>
```

## What the Analysis Contains

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
  - speaker
  - speaker role
  - topic
  - emotion
- sample utterance tags:
  - first 8 items from the full tagging output, kept for quick inspection
- therapist recommendations:
  - positive patient topics
  - negative patient topics
  - short suggestions for the next session

## Storage Layout in MinIO

The `videos` bucket is used as the main storage bucket.

- `raw/...mp4`: uploaded source videos
- `audio/...mp3`: extracted audio files
- `transcripts/...json`: AssemblyAI transcription results
- `analysis/...json`: final analysis output

## Queues in RabbitMQ

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

- connect with `DD_API_KEY`
- collect all container logs
- tail Docker container log files

This satisfies the assignment requirement that logs from all services are shown centrally in Datadog.

## Important Implementation Notes

- Internal service-to-service communication is event-driven through RabbitMQ, not REST.
- The only user-facing APIs are `upload_api` and `query_api`.
- LLM-based speaker roles and utterance tags are cached in Redis to avoid repeated Gemini calls.
- The analysis worker also caches the basic transcript analysis.
- Datadog log collection was verified from container logs inside Datadog.

## What Still Needs To Be Done Before Submission

Code-wise, the system is already close to the required minimum. Before submitting, the following should still be completed:

1. Run one clean end-to-end demo from upload to query results.
2. Prepare a screen recording that shows:
   - upload
   - MinIO artifacts
   - RabbitMQ queues
   - query API responses
   - Datadog logs
3. Zip the exercise code according to the assignment naming instructions.
4. Upload the demo video to Google Drive.
5. Fill in the Google Form from the assignment instructions.

## Suggested Demo Flow

For the recording, show:

1. `docker compose up -d --build`
2. Upload a therapy session video
3. MinIO showing raw video, MP3, transcript JSON, and analysis JSON
4. RabbitMQ queues receiving and consuming events
5. Query API returning `/videos` and `/videos/{id}`
6. Datadog Live Tail showing logs from the containers

## Recommended Sample Input

Use a short English therapy or mock therapy session video with:

- at least two speakers
- clear audio
- little or no music
- public availability, for example a YouTube mock therapy session

## Submission Reminder

According to the assignment instructions, remember to:

- keep meaningful commits in GitHub
- create the required zip file for Exercise 1
- record a working demo video
- upload the video to Google Drive
- submit the links and required details in the course form
