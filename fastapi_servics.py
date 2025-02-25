# fastapi_service.py
from fastapi import FastAPI
from kafka import KafkaConsumer
import json
import asyncio
import aiohttp
from typing import List, Dict
import uvicorn

app = FastAPI()

# Kafka consumer setup
user_consumer = KafkaConsumer(
    'user_data',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

job_consumer = KafkaConsumer(
    'job_data',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# URLs for job and resume services
JOBS_SERVICE_URL = "http://localhost:8001"
RESUME_SERVICE_URL = "http://localhost:8002"

# Background task to consume Kafka messages and send to services
async def consume_kafka():
    async with aiohttp.ClientSession() as session:
        while True:
            # Consume user data
            for msg in user_consumer:
                data = msg.value
                user_id = data["user_id"]
                resume_text = data["resume_text"]
                skillset = data.get("skillset", "")
                payload = {"user_id": user_id, "resume_text": resume_text, "skillset": skillset}
                async with session.post(f"{RESUME_SERVICE_URL}/upload_resume", json=payload) as resp:
                    if resp.status == 200:
                        print(f"Uploaded resume for {user_id}")
                    else:
                        print(f"Failed to upload resume for {user_id}: {await resp.text()}")
                break

            # Consume job data
            for msg in job_consumer:
                data = msg.value
                job_id = data["job_id"]
                job_text = data["job_text"]
                payload = {"job_id": job_id, "job_text": job_text}
                async with session.post(f"{JOBS_SERVICE_URL}/upload_job", json=payload) as resp:
                    if resp.status == 200:
                        print(f"Uploaded job {job_id}")
                    else:
                        print(f"Failed to upload job {job_id}: {await resp.text()}")
                break

            await asyncio.sleep(1)  # Avoid tight loop

@app.on_event("startup")
async def startup_event():
    """Start Kafka consumer on app startup."""
    asyncio.create_task(consume_kafka())

@app.get("/matches/{user_id}", response_model=Dict[str, List[Dict]])
async def get_job_matches(user_id: str):
    """Endpoint to get job matches for a user, served to homepage."""
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{RESUME_SERVICE_URL}/match/{user_id}") as resp:
            if resp.status == 200:
                matches = await resp.json()
                return {
                    "matches": [
                        {"job_id": match["job_id"], "job_text": match["job_text"], "score": match["score"] * 100}
                        for match in matches
                    ]
                }
            elif resp.status == 404:
                return {"matches": [], "error": "Resume not found"}
            else:
                return {"matches": [], "error": f"Error fetching matches: {await resp.text()}"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)