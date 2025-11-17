import json
import psycopg
from kafka import KafkaConsumer


def run():
    print("[Consumer] Connecting to Kafka...")
    consumer = KafkaConsumer(
        "deliveries",
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id="delivery-consumer-group",
    )
    print("[Consumer] Connected ✓")

    print("[Consumer] Connecting to PostgreSQL...")
    conn = psycopg.connect(
        dbname="kafka_db",
        user="kafka_user",
        password="kafka_password",
        host="localhost",
        port="5433",
    )
    conn.autocommit = True
    cur = conn.cursor()
    print("[Consumer] PostgreSQL Connected ✓")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS deliveries (
            delivery_id VARCHAR(20) PRIMARY KEY,
            restaurant VARCHAR(100),
            cuisine VARCHAR(50),
            region VARCHAR(50),
            delivery_distance_km NUMERIC(6,2),
            delivery_time_estimate_min INT,
            driver_rating NUMERIC(2,1),
            surge_multiplier NUMERIC(3,1),
            timestamp TIMESTAMP
        );
    """
    )
    print("[Consumer] Table 'deliveries' ready ✓")

    count = 0
    for msg in consumer:
        data = msg.value
        try:
            cur.execute(
                """
                INSERT INTO deliveries (
                    delivery_id, restaurant, cuisine, region,
                    delivery_distance_km, delivery_time_estimate_min,
                    driver_rating, surge_multiplier, timestamp
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (delivery_id) DO NOTHING;
            """,
                (
                    data["delivery_id"],
                    data["restaurant"],
                    data["cuisine"],
                    data["region"],
                    data["delivery_distance_km"],
                    data["delivery_time_estimate_min"],
                    data["driver_rating"],
                    data["surge_multiplier"],
                    data["timestamp"],
                ),
            )
            count += 1
            print(f"[Consumer] Inserted #{count} → {data['delivery_id']}")
        except Exception as e:
            print(f"[Consumer ERROR] {e}")


if __name__ == "__main__":
    run()
