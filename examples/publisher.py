from nats_repository import ConnectionParameters
from nats_repository import Publisher
import asyncio
import logging
import json
import time

logger = logging.getLogger(__name__)


async def main():
    connection_parameters = ConnectionParameters(
        servers=['nats://localhost:4222'],
        username='YOUR_NATS_USERNAME',
        password='YOUR_NATS_PASSWORD'
    )
    subjects = ['user_signup']
    publisher = Publisher(
        connection_parameters=connection_parameters,
        stream_name='events',
        subjects=subjects,
    )
    await publisher.start()

    benchmark_intervals = [1000, 10000, 100000, 1000000]
    for count in benchmark_intervals:
        start = time.perf_counter()
        for i in range(count):
            user = {
                'id': i + 1,
                'email': f'user_{i+1}@example.com'
            }
            payload = json.dumps(user)
            try:
                await publisher.publish(
                    payload=payload,
                    subject='user_signup'
                )
            except Exception as e:
                logger.error(f"Error publishing message: {e}")
                break

        end = time.perf_counter()
        print(f'Time taken for {count} events to publish: {end - start} seconds.')
    await publisher.stop()

if __name__ == '__main__':
    asyncio.run(main())
