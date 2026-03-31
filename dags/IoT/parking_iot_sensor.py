import argparse
import json
import time
import random
import asyncio
from datetime import datetime
from kafka import KafkaProducer


def init_producer(bootstrap_servers: list[str]):
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )


async def simulate(device_id: str, producer: KafkaProducer, topic_name: str):
    is_empty = random.choice([True,False])
    try:
        while True:
            sleep = random.uniform(1.0, 10.0)
            await asyncio.sleep(sleep)

            is_empty = not is_empty

            event = {
                "device_id": device_id,
                "timestamp": datetime.utcnow().isoformat(),
                "state": "empty" if is_empty else "full",
            }
            
            producer.send(topic_name, value=event)
            producer.flush()
            print(f"[{event['timestamp']}] Event generated: Lot {event['device_id']} now is {event['state']}.")
            
    except KeyboardInterrupt:
        print("\nSimulation stopped by user.")
    finally:
        producer.flush()
        producer.close()

async def main():
    parser = argparse.ArgumentParser(description="Kafka Device Simulator")

    parser.add_argument(
        '-n', '--number-of-devices',
        type=int,
        nargs=1, 
        default=10000, 
    )

    parser.add_argument(
        '-b', '--brokers', 
        type=str,
        nargs='+', 
        required=True,
    )

    parser.add_argument(
        '-t', '--topic', 
        type=str, 
        nargs=1,
        default='parking-lot-events', 
    )

    args = parser.parse_args()
    n = args.number_of_devices
    kafka_brokers = args.brokers
    topic_name = args.topic

    devices_bcn = [f"P_{i}" for i in range(n)]

    current_tasks = []

    kafka_producer = init_producer(kafka_brokers)

    for device in devices_bcn:
        task = asyncio.create_task(simulate(device, kafka_producer, topic_name))
        current_tasks.append(task)

    await asyncio.gather(*current_tasks)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass