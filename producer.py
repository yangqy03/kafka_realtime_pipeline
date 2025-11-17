import time
import json
import random
import uuid
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer

fake = Faker()

CUISINES = ["Burgers", "BBQ", "Sushi", "Chinese", "Pizza", "Indian", "Mexican", "Vegan"]
REGIONS = ["Downtown", "Uptown", "Suburbs", "Airport", "Campus", "Waterfront"]
RESTAURANTS = [
    "Flavor Town",
    "Dragon Palace",
    "Urban Grill",
    "Sushi House",
    "Taco Fiesta",
    "Green Garden",
    "Pizza Bros",
    "BBQ Pit",
]


def generate_delivery():
    cuisine = random.choice(CUISINES)
    region = random.choice(REGIONS)
    restaurant = random.choice(RESTAURANTS)

    delivery_time = random.randint(15, 55)
    distance = round(random.uniform(0.5, 12.0), 2)
    rating = round(random.uniform(3.0, 5.0), 1)
    surge = round(random.choice([1.0, 1.1, 1.2, 1.4, 1.7, 2.0]), 1)

    return {
        "delivery_id": str(uuid.uuid4())[:8],
        "restaurant": restaurant,
        "cuisine": cuisine,
        "region": region,
        "delivery_distance_km": distance,
        "delivery_time_estimate_min": delivery_time,
        "driver_rating": rating,
        "surge_multiplier": surge,
        "timestamp": datetime.now().isoformat(),
    }


def run():
    print("[Producer] Connecting to Kafka...")
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    print("[Producer] Connected ✓")

    count = 0

    while True:
        event = generate_delivery()
        producer.send("deliveries", value=event)
        producer.flush()
        print(f"[Producer] Sent event #{count} → {event}")
        count += 1

        time.sleep(random.uniform(0.5, 2.0))


if __name__ == "__main__":
    run()
