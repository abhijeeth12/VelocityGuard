import json
import time
import random
import uuid
from kafka import KafkaProducer
from datetime import datetime

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'raw_payments'

def get_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8')
        )
        return producer
    except Exception as e:
        print(f"Error connecting to Kafka: {e}")
        return None

def generate_transaction():
    tx_id = str(uuid.uuid4())
    user_id = f"user_{random.randint(1, 100)}"
    # Skew amount to sometimes generate fraudulent transactions (> 5000)
    if random.random() < 0.05:
        amount = round(random.uniform(5001, 10000), 2)
    else:
        amount = round(random.uniform(10, 1000), 2)
    
    timestamp = datetime.utcnow().isoformat()
    locations = ['US', 'CA', 'UK', 'IN', 'AU', 'DE', 'FR']
    location = random.choice(locations)
    
    return {
        "tx_id": tx_id,
        "user_id": user_id,
        "amount": amount,
        "timestamp": timestamp,
        "location": location
    }

def start_producing():
    producer = get_producer()
    if not producer:
        return
    
    print(f"Producing messages to topic '{TOPIC_NAME}'...")
    try:
        while True:
            tx = generate_transaction()
            producer.send(TOPIC_NAME, key=tx["user_id"], value=tx)
            print(f"Sent: {tx}")
            time.sleep(random.uniform(0.1, 1.0))
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.close()

if __name__ == "__main__":
    start_producing()
