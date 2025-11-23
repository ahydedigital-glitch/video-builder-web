import os
import uuid
import json
import requests
from fastapi import FastAPI, Form
from fastapi.responses import JSONResponse

app = FastAPI()

# ====================================================
# CONFIG — Cloudflare Queues
# ====================================================
CF_ACCOUNT_ID = os.getenv("CF_ACCOUNT_ID")
CF_QUEUE_NAME = os.getenv("CF_QUEUE_NAME")
CF_API_TOKEN = os.getenv("CF_API_TOKEN")

required = [CF_ACCOUNT_ID, CF_QUEUE_NAME, CF_API_TOKEN]
if not all(required):
    raise RuntimeError("Missing CF_ACCOUNT_ID, CF_QUEUE_NAME, or CF_API_TOKEN")

QUEUE_URL = (
    f"https://api.cloudflare.com/client/v4/accounts/"
    f"{CF_ACCOUNT_ID}/queues/{CF_QUEUE_NAME}/messages"
)

HEADERS = {
    "Authorization": f"Bearer {CF_API_TOKEN}",
    "Content-Type": "application/json",
}


# ====================================================
# FUNCTION — Push a job into Cloudflare Queue
# ====================================================
def enqueue_job(job: dict):
    payload = {
        "messages": [{"body": job}]
    }

    resp = requests.post(
        QUEUE_URL,
        headers=HEADERS,
        json=payload,
        timeout=15
    )

    if resp.status_code >= 300:
        raise RuntimeError(
            f"Cloudflare Queue error: status={resp.status_code}, body={resp.text}"
        )

    data = resp.json()
    if not data.get("success", False):
        raise RuntimeError(f"Cloudflare Queue returned error: {json.dumps(data)}")


# ====================================================
# API ENDPOINT — Submit a video job
# ====================================================
@app.post("/video-url")
async def queue_video(
    audio_url: str = Form(...),
    image_url: str = Form(...),
    date: str = Form(...)
):
    job_id = uuid.uuid4().hex
    final_key = f"final-video-{date}-{job_id[:8]}.mp4"

    job = {
        "job_id": job_id,
        "audio_url": audio_url,
        "image_url": image_url,
        "date": date,
        "final_key": final_key,
    }

    enqueue_job(job)

    return JSONResponse({
        "status": "queued",
        "job_id": job_id,
        "video_file": final_key,
        "message": "Job queued. Worker will handle processing."
    })


@app.get("/")
def health():
    return {"status": "ok", "service": "video-builder-web"}
