# job_producer.py
from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def send_job_data(job_id, job_text):
    job_data = {"job_id": job_id, "job_text": job_text}
    producer.send('job_data', value=job_data)
    print(f"Sent job data for {job_id}")

if __name__ == "__main__":
    send_job_data("job456", "Hiring a Software Engineer proficient in Python.")
    time.sleep(1)
    producer.flush()