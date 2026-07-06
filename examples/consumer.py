from nats_repository import ConnectionParameters
from nats_repository import Consumer
import asyncio
import logging
import time

logger = logging.getLogger(__name__)


class BenchmarkConsumer:
    def __init__(self):
        self.event_count = 0
        self.benchmark_intervals = [1000, 10000, 100000, 1000000]
        self.start_time = time.time()

    async def consume(self, event, subject, retries):
        self.event_count += 1

        if self.event_count in self.benchmark_intervals:
            elapsed_time = time.time() - self.start_time
            print(f"Time to consume {self.event_count} events: {elapsed_time:.2f} seconds")

async def main():
    connection_parameters = ConnectionParameters(
        # servers=['nats://localhost:4222/'],
        servers=['nats://localhost:4222'],
        username='YOUR_NATS_USERNAME',
        password='YOUR_NATS_PASSWORD'
    )

    consumer = Consumer(
        connection_parameters=connection_parameters,
        stream_name='events',
        subjects=['events'],
        max_retries=5,
        backoff_intervals=[1_000_000_000, 5_000_000_000],
        dead_letter_subject='events_dead_letter'
    )

    benchmark_consumer = BenchmarkConsumer()
    consumer.callback = benchmark_consumer.consume
    await consumer.start()

if __name__ == '__main__':
    asyncio.run(main())
