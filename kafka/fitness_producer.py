from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

user_ids = ['U101', 'U102', 'U103', 'U104', 'U105', 'U106', 'U107', 'U108', 'U109', 'U110']

def generate_data():
    return {
        "user_id": random.choice(user_ids),
        "timestamp": datetime.utcnow().isoformat(),
        "heart_rate": random.randint(60, 180),
        "steps": random.randint(10, 100),
        "calories_burned": round(random.uniform(0.5, 3.0), 2)
    }

if __name__ == "__main__":
    while True:
        data = generate_data()
        producer.send('fitness_raw', value=data)
        print(f"Sent: {data}")
        time.sleep(2)

