# jobs_service.py
from fastapi import FastAPI, HTTPException
import spacy
import pymongo
from sentence_transformers import SentenceTransformer
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import uvicorn

app = FastAPI()

# Load NLP model and Sentence Transformer
nlp = spacy.load("en_core_web_sm")
model = SentenceTransformer("sentence-transformers/all-mpnet-base-v2")

# MongoDB Connection
uri = "mongodb+srv://tejarachakonda:teja123@cluster0.9agko.mongodb.net/"
client = MongoClient(uri, server_api=ServerApi('1'))
db = client["job_matching"]
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

@app.post("/upload_job")
async def upload_job(job_id: str, job_text: str):
    """Endpoint to upload and process a job description."""
    if not job_text:
        raise HTTPException(status_code=400, detail="Job text cannot be empty")
    
    clean_text = preprocess_text(job_text)
    embedding = get_embedding(clean_text)
    job_data = {
        "file_name": f"{job_id}.txt",
        "text": clean_text,
        "embedding": embedding
    }
    jobs_collection.insert_one(job_data)
    return {"message": f"Job {job_id} uploaded and stored successfully"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)