# MaritimeIQ cloud deploy notes

Local Spark remains the default. Use GCP / Daytona for **shareable demos**, not day-to-day development.

## Daytona

1. Create a workspace from this repository.  
2. Add secret `OPENAI_API_KEY` (or Vertex settings).  
3. Run:

```bash
make install
make demo
make ui
```

4. Forward / open port **8501**.

See [`daytona.yaml`](daytona.yaml) for a workspace hint.

## Docker (local or Cloud Run base)

```bash
# Build (needs data/hackathon present)
docker build -t maritimeiq .

# UI against an existing lakehouse volume
docker run --rm -p 8501:8501 --env-file .env \
  -v "$PWD/lakehouse:/app/lakehouse" \
  -v "$PWD/data:/app/data:ro" \
  maritimeiq
```

For a fast Cloud Run demo, run `make demo` locally (or in Daytona), sync lakehouse to GCS, then serve the UI container with that volume/bucket mount.

## GCS artifact sync

```bash
export GCS_BUCKET=gs://your-bucket/maritimeiq
./scripts/sync_gcs.sh
```

Uploads `lakehouse/` and `.eval_results/` (if present).

## Vertex AI (optional LLM)

In `.env`:

```bash
MARITIMEIQ_LLM_PROVIDER=vertex
MARITIMEIQ_MODEL=google/gemini-2.0-flash
VERTEX_OPENAI_BASE_URL=https://YOUR_VERTEX_OPENAI_COMPAT_ENDPOINT/v1
OPENAI_API_KEY=YOUR_VERTEX_TOKEN
# or GOOGLE_CLOUD_PROJECT=... for documentation / ADC
```

The client uses an OpenAI-compatible Vertex endpoint when `VERTEX_OPENAI_BASE_URL` / `OPENAI_BASE_URL` is set.

## Cloud Run (recipe)

1. `make demo` → produce `lakehouse/`  
2. `./scripts/sync_gcs.sh`  
3. Build and push image: `docker build -t REGION-docker.pkg.dev/PROJECT/maritimeiq/ui:latest . && docker push …`  
4. Deploy Cloud Run service with:

   - Port `8501`  
   - Env from Secret Manager (`OPENAI_API_KEY`)  
   - Startup that downloads lakehouse from GCS into `/app/lakehouse` (init container or entrypoint wrapper)

Minimal entrypoint wrapper example:

```bash
gsutil -m rsync -r "$GCS_BUCKET/lakehouse" /app/lakehouse
exec python3 -m streamlit run src/maritimeiq/ui/app.py --server.port=8501 --server.address=0.0.0.0
```

Skip Dataproc / BigQuery / Kafka for this showcase — they burn credits without changing the agent story.
