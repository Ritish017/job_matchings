# resume_service.py
from fastapi import FastAPI, HTTPException
import spacy
import pymongo
import numpy as np
from sentence_transformers import SentenceTransformer
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from typing import List, Dict
import uvicorn

app = FastAPI()

# Load NLP model and Sentence Transformer
nlp = spacy.load("en_core_web_sm")
model = SentenceTransformer("sentence-transformers/all-mpnet-base-v2")

# MongoDB Connection
uri = "mongodb+srv://tejarachakonda:teja123@cluster0.9agko.mongodb.net/"
client = MongoClient(uri, server_api=ServerApi('1'))
db = client["job_matching"]
resumes_collection = db["resumes"]
jobs_collection = db["jobs"]

def preprocess_text(text: str) -> str:
    """Cleans text by lemmatizing, removing stopwords, and including key named entities."""
    doc = nlp(text)
    tokens = [token.lemma_ for token in doc if not token.is_stop and not token.is_punct]
    named_entities = [ent.text.lower() for ent in doc.ents if ent.label_ in ["ORG", "PERSON", "GPE", "DATE", "WORK_OF_ART"]]
    return " ".join(tokens + named_entities)

def get_embedding(text: str) -> list:
    """Gets sentence embedding as a list."""
    return model.encode(text).tolist()

def cosine_similarity(vec1: list, vec2: list) -> float:
    """Calculates cosine similarity between two vectors."""
    vec1 = np.array(vec1)
    vec2 = np.array(vec2)
    if np.linalg.norm(vec1) == 0 or np.linalg.norm(vec2) == 0:
        return 0
    return np.dot(vec1, vec2) / (np.linalg.norm(vec1) * np.linalg.norm(vec2))

@app.post("/upload_resume")
async def upload_resume(user_id: str, resume_text: str, skillset: str = ""):
    """Endpoint to upload and process a resume."""
    if not resume_text:
        raise HTTPException(status_code=400, detail="Resume text cannot be empty")
    
    combined_text = resume_text + " " + skillset
    clean_resume = preprocess_text(combined_text)
    resume_embedding = get_embedding(clean_resume)
    resume_data = {
        "file_name": f"{user_id}_resume.txt",
        "text": clean_resume,
        "embedding": resume_embedding,
        "skillset": skillset
    }
    resumes_collection.insert_one(resume_data)
    return {"message": f"Resume for {user_id} uploaded and stored successfully"}

@app.get("/match/{user_id}", response_model=List[Dict])
async def match_jobs(user_id: str):
    """Endpoint to compute and return job matches for a user."""
    latest_resume = resumes_collection.find({"file_name": f"{user_id}_resume.txt"}).sort([('_id', -1)]).limit(1)
    latest_resume = list(latest_resume)
    if not latest_resume:
        raise HTTPException(status_code=404, detail="Resume not found")
    
    resume_embedding = latest_resume[0]["embedding"]
    jobs = list(jobs_collection.find())
    if not jobs:
        return []
    
    similarities = []
    for job in jobs:
        sim = cosine_similarity(resume_embedding, job["embedding"])
        file_name = job.get("file_name", "Unknown Job")
        similarities.append({"job_id": file_name.split(".")[0], "job_text": job["text"], "score": sim})
    
    similarities.sort(key=lambda x: x["score"], reverse=True)
    return similarities[:5]

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8002)