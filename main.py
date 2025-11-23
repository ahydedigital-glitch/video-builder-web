import os
import uuid
import json
import requests
from fastapi import FastAPI, Form
from fastapi.responses import JSONResponse

app = FastAPI()

# ====================================================
# CONFIG: Cloudflare Queues
# ====================================================
CF_ACCOUNT_ID = os.getenv("CF_ACCOUNT_ID")
CF_QUEUE_NAME = os.getenv("CF_QUEUE_NAME")  # this is the *Queue name* in Cloudflare, not the ID
CF_API_TOKEN = os.getenv("CF_API_TOKEN")    # API token with Queues write permission

if not CF_ACCOUNT_ID or not CF_QUEUE_NAME or not CF_API_TOKEN:
    raise RuntimeError(
        "Missing one or more Cloudflare env vars: CF_ACCOUNT_ID, CF_QUEUE_NAME, CF_API_TOKEN"
    )

QUEUE_URL = (
    f"https://api.cloudflare.com/client/v4/accounts/"
    f"{CF_ACCOUNT_ID}/queues/{CF_QUEUE_NAME}/messages"
)


def enqueue_job(job: dict) -> None:
    """
    Push a job into Cloudflare Queues.
    The worker will consume and process it.
    """
    headers = {
        "Authorization": f"Bearer {CF_API_TOKEN}",
        "Content-Type": "application/json",
    }

    # Cloudflare Queues accepts a batch of messages.
    # Each message's `body` is arbitrary JSON.
    payload = {
        "messages": [
            {
                "body": job
            }
        ]
    }

    resp = requests.post(QUEUE_URL, headers=headers, json=payload, timeout=10)
    try:
        resp.raise_for_status()
    except Exception as exc:
        # Include Cloudflare's response body for easier debugging
        raise RuntimeError(
            f"Failed to enqueue job. Status={resp.status_code}, Body={resp.text}"
        ) from exc

    data = resp.json()
    if not data.get("success", True):
        # Cloudflare-style error wrapper
        raise RuntimeError(f"Cloudflare Queues error: {json.dumps(data)}")


# ====================================================
# API ENDPOINTS
# ====================================================
@app.post("/video-url")
async def queue_video(
    audio_url: str = Form(...),
    image_url: str = Form(...),
    date: str = Form(...),
):
    """
    Accepts URLs to the merged audio + image, and queues a job
    for the worker to actually build the video.

    This endpoint returns immediately with a job_id and final_key.
    """

    job_id = str(uuid.uuid4())
    # This is the object key the *worker* should use when uploading to R2.
    final_key = f"final-video-{date}-{job_id[:8]}.mp4"

    job = {
        "job_id": job_id,
        "audio_url": audio_url,
        "image_url": image_url,
        "date": date,
        "final_key": final_key,
    }

    enqueue_job(job)

    return JSONResponse(
        {
            "status": "queued",
            "job_id": job_id,
            "video_file": final_key,
            "message": "Job queued. Worker will build and upload the video.",
        }
    )


@app.get("/")
def health():
    return {"status": "ok", "service": "video-builder-web"}
