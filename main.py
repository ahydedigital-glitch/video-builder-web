import os
import uuid
import json
import requests
from fastapi import FastAPI, Form, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Video Builder Web API")

# Enable CORS for n8n integration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ====================================================
# CONFIG — Cloudflare Queues
# ====================================================
CF_ACCOUNT_ID = os.getenv("CF_ACCOUNT_ID")
CF_QUEUE_ID = os.getenv("CF_QUEUE_ID")
CF_QUEUE_NAME = os.getenv("CF_QUEUE_NAME", "video-jobs")
CF_API_TOKEN = os.getenv("CF_API_TOKEN")

required = [CF_ACCOUNT_ID, CF_QUEUE_ID, CF_API_TOKEN]
if not all(required):
    raise RuntimeError("Missing CF_ACCOUNT_ID, CF_QUEUE_ID, or CF_API_TOKEN")

# CHANGED: Use /messages/push endpoint
QUEUE_URL = (
    f"https://api.cloudflare.com/client/v4/accounts/"
    f"{CF_ACCOUNT_ID}/queues/{CF_QUEUE_ID}/messages/batch"
)

HEADERS = {
    "Authorization": f"Bearer {CF_API_TOKEN}",
    "Content-Type": "application/json",
}

print(f"✅ Configuration loaded:")
print(f"   Account ID: {CF_ACCOUNT_ID}")
print(f"   Queue ID: {CF_QUEUE_ID}")
print(f"   Queue Name: {CF_QUEUE_NAME}")
print(f"   Queue URL: {QUEUE_URL}")


# ====================================================
# FUNCTION — Push a job into Cloudflare Queue
# ====================================================
def enqueue_job(job: dict):
    """Send a job to Cloudflare Queue for processing."""
    payload = {
        "messages": [{"body": job}]
    }

    print(f"\n📤 Sending job to queue:")
    print(f"   Job ID: {job.get('job_id')}")
    print(f"   Audio URL: {job.get('audio_url')}")
    print(f"   Image URL: {job.get('image_url')}")

    try:
        resp = requests.post(
            QUEUE_URL,
            headers=HEADERS,
            json=payload,
            timeout=15
        )

        print(f"   Response Status: {resp.status_code}")

        if resp.status_code >= 300:
            print(f"❌ Cloudflare Queue error: {resp.text}")
            raise HTTPException(
                status_code=502,
                detail=f"Cloudflare Queue error: status={resp.status_code}, body={resp.text}"
            )

        data = resp.json()
        if not data.get("success", False):
            print(f"❌ Cloudflare Queue returned error: {json.dumps(data)}")
            raise HTTPException(
                status_code=502,
                detail=f"Cloudflare Queue returned error: {json.dumps(data)}"
            )

        print(f"✅ Job queued successfully")
        return data

    except requests.RequestException as e:
        print(f"❌ Request failed: {e}")
        raise HTTPException(
            status_code=502,
            detail=f"Failed to connect to Cloudflare Queue: {str(e)}"
        )


# ====================================================
# API ENDPOINT — Submit a video job
# ====================================================
@app.post("/video-url")
async def queue_video(
    audio_url: str = Form(...),
    image_url: str = Form(...),
    date: str = Form(...)
):
    """
    Queue a video creation job.
    
    Args:
        audio_url: Full URL to the MP3 audio file in R2
        image_url: Full URL to the image file in R2
        date: Date string for the filename (e.g., "2025-01-24")
    
    Returns:
        JSON response with job details
    """
    job_id = uuid.uuid4().hex
    final_key = f"final-video-{date}.mp4"

    job = {
        "job_id": job_id,
        "audio_url": audio_url,
        "image_url": image_url,
        "date": date,
        "final_key": final_key,
    }

    # Send to queue
    enqueue_job(job)

    return JSONResponse({
        "status": "queued",
        "job_id": job_id,
        "video_file": final_key,
        "message": "Job queued successfully. Worker will process it.",
        "estimated_location": f"https://your-r2-bucket.com/{final_key}"
    })


@app.get("/")
def health():
    """Health check endpoint."""
    return {
        "status": "ok",
        "service": "video-builder-web",
        "version": "2.0",
        "queue": CF_QUEUE_NAME
    }


@app.get("/health")
def health_detailed():
    """Detailed health check with configuration."""
    return {
        "status": "ok",
        "service": "video-builder-web",
        "cloudflare_account": CF_ACCOUNT_ID,
        "queue_id": CF_QUEUE_ID,
        "queue_name": CF_QUEUE_NAME,
        "queue_url": QUEUE_URL,
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
