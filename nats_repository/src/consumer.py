from nats_repository.src.connection_parameters import ConnectionParameters
from nats.js.api import DeliverPolicy, AckPolicy, ConsumerConfig, StreamConfig
from nats.js.client import JetStreamContext
from nats.aio.client import Client as NATS
from multiprocessing import Process
from typing import Callable, List
import asyncio
import logging
import signal
import json

logger = logging.getLogger(__name__)


class Consumer(Process):
    """
    A Consumer for NATS JetStream to process messages from a stream.

    Parameters:
        connection_parameters (ConnectionParameters):
            The connection details for the NATS server (e.g., servers, authentication).
        stream_name (str):
            The name of the JetStream to consume messages from.
        subjects (List[str]):
            A list of subjects to subscribe to for consuming messages.
        max_retries (int, optional):
            The maximum number of retry attempts for processing a message before
            moving it to the dead-letter subject (if configured). Default is 5.
        backoff_intervals (List[float], optional):
            A list of backoff intervals (in seconds) to wait before retrying
            message processing. Default is None (no backoff).
        dead_letter_subject (str, optional):
            The subject to which messages exceeding max retries will be sent.
            Default is None (no dead-letter subject).
        concurrency (int, optional):
            The maximum number of concurrent message processing tasks. Default is 100.
        fetch_timeout (int, optional):
            The timeout (in seconds) for fetching messages from the JetStream.
            Default is 1 second.
        msg_retention (str, optional):
            The message retention policy for the JetStream stream ('workqueue',
            'limits', or 'interest'). Default is 'workqueue'.
        msg_max_count (int, optional):
            The maximum number of messages to retain in the stream. Default is 1,000,000,000.
        msg_max_size (int, optional):
            The maximum size of the stream in **GB**. Default is 1 GB.

    Example:
        consumer = Consumer(
            connection_parameters=ConnectionParameters(...),
            stream_name="example_stream",
            subjects=["example_subject"],
            max_retries=3,
            backoff_intervals=[1, 5, 10],  # Retry after 1s, 5s, and 10s
            dead_letter_subject="example_dead_letter",
            concurrency=10,
            fetch_timeout=2,
            msg_retention="limits",
            msg_max_count=100_000,
            msg_max_size=1  # 1 GB
        )
        await consumer.start()
    """

    def __init__(
        self,
        connection_parameters: ConnectionParameters,
        stream_name: str,
        subjects: List[str],
        max_retries: int = 5,
        backoff_intervals: List[float] = None,
        dead_letter_subject: str = None,
        concurrency: int = 100,
        fetch_timeout: int = 1,
        msg_retention: str = 'workqueue',
        msg_max_count: int = 1_000_000_000,
        msg_max_size: int = 1
    ):
        self._connection_parameters = connection_parameters
        self._stream_name = stream_name
        self._subjects = subjects
        self._worker_tasks = []
        self._nats_client: NATS = NATS()
        self._jetstream: JetStreamContext = None
        self._concurrency = int(concurrency)
        self.max_queue_size = self._concurrency * 4
        self._fetch_timeout = fetch_timeout
        self._msg_retention = msg_retention
        self._msg_max_count = int(msg_max_count)
        self._msg_max_size = int(msg_max_size * 1024 * 1024 * 1024)
        self._semaphore = asyncio.Semaphore(self._concurrency)
        self._message_queue = asyncio.Queue(maxsize=self.max_queue_size)
        self._max_retries = max_retries
        self._backoff_intervals = backoff_intervals or []
        self._dead_letter_subject = dead_letter_subject

        self.callback: Callable = (
            lambda event, subject, retries: logger.info(
                f"Processing event: {event} from {subject} with retries {retries}"
            )
        )
        self._stop_event = asyncio.Event()

        self.__loop = None
        super().__init__()

    async def start_task(self):
        self._register_signal_handlers()
        await self.connect_and_consume()

    def _register_signal_handlers(self):
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(self._shutdown(s)))

    async def _shutdown(self, sig):
        logger.info(f"Received exit signal {sig.name}...")
        self._stop_event.set()
        await self.stop()

    async def connect_and_consume(self):
        options = {
            "servers": self._connection_parameters.servers,
            "max_reconnect_attempts": self._connection_parameters.max_reconnect_attempts,
            "reconnect_time_wait": self._connection_parameters.reconnect_time_wait,
        }
        if self._connection_parameters.username and self._connection_parameters.password:
            options["user"] = self._connection_parameters.username
            options["password"] = self._connection_parameters.password
        elif self._connection_parameters.token:
            options["token"] = self._connection_parameters.token

        await self._nats_client.connect(**options)
        logger.info('Connected to NATS.')

        self._jetstream = self._nats_client.jetstream()
        await self.create_stream_if_not_exists()
        await self.subscribe()

        for _ in range(self._concurrency):
            task = asyncio.create_task(self._process_messages())
            self._worker_tasks.append(task)

        await self._stop_event.wait()
        await self._message_queue.join()

        for task in self._worker_tasks:
            task.cancel()
        await asyncio.gather(*self._worker_tasks, return_exceptions=True)

    async def create_stream_if_not_exists(self):
        try:
            await self._jetstream.stream_info(self._stream_name)
            logger.info(f"Stream {self._stream_name} already exists.")
        except Exception as e:
            logger.error(f"Error fetching stream info: {e}")
            stream_config = StreamConfig(
                name=self._stream_name,
                subjects=self._subjects,
                storage='file',
                retention=self._msg_retention,
                max_msgs=self._msg_max_count,
                max_bytes=self._msg_max_size
            )
            await self._jetstream.add_stream(stream_config)
            logger.info(f"Stream {self._stream_name} created.")

        if self._dead_letter_subject:
            try:
                await self._jetstream.stream_info(self._dead_letter_subject)
                logger.info(f"Dead-letter subject stream {self._dead_letter_subject} already exists.")
            except Exception as e:
                logger.error(f"Error fetching dead-letter stream info: {e}")
                dlq_stream_config = StreamConfig(
                    name=self._dead_letter_subject,
                    subjects=[self._dead_letter_subject],
                    storage='file',
                    retention=self._msg_retention,
                    max_msgs=self._msg_max_count,
                    max_bytes=self._msg_max_size
                )
                await self._jetstream.add_stream(dlq_stream_config)
                logger.info(f"Dead-letter subject stream {self._dead_letter_subject} created.")

    async def subscribe(self):
        for subject in self._subjects:
            replacements = str.maketrans({
                ".": "_",
                "*": "_",
                ">": "_"
            })
            new_subject = subject.translate(replacements)

            try:
                consumer_config = ConsumerConfig(
                    durable_name=f"{new_subject}_durable",
                    deliver_policy=DeliverPolicy.ALL,
                    ack_policy=AckPolicy.EXPLICIT,
                    max_deliver=self._max_retries,
                    backoff=self._backoff_intervals,
                    filter_subject=subject,
                    ack_wait=30,
                )

                try:
                    await self._jetstream.consumer_info(self._stream_name, consumer_config.durable_name)
                    logger.info(f"Consumer {consumer_config.durable_name} already exists.")
                except Exception:
                    await self._jetstream.add_consumer(
                        stream=self._stream_name,
                        config=consumer_config,
                    )
                    logger.info(f"Consumer {consumer_config.durable_name} created.")

                sub = await self._jetstream.pull_subscribe(
                    subject=subject,
                    durable=consumer_config.durable_name,
                )
                asyncio.create_task(self.consume_messages(sub))

                logger.info(f"Subscribed to subject: {subject}")
            except Exception as e:
                logger.error(f"Failed to subscribe to subject {subject}: {e}", exc_info=True)

    async def consume_messages(self, sub):
        while not self._stop_event.is_set():
            try:
                fetch_size = self._message_queue.maxsize - self._message_queue.qsize()
                if fetch_size <= 0:
                    await asyncio.sleep(0.1)
                    continue
                messages = await sub.fetch(fetch_size, timeout=self._fetch_timeout)
                for msg in messages:
                    if self._stop_event.is_set():
                        break
                    await self._message_queue.put(msg)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Error fetching messages: {e}", exc_info=True)

    async def _process_messages(self):
        while not self._stop_event.is_set() or not self._message_queue.empty():
            try:
                msg = await self._message_queue.get()
                await self.handle_message(msg)
            except Exception as e:
                logger.error(f"Error processing message: {e}", exc_info=True)
            finally:
                self._message_queue.task_done()

    async def handle_message(self, msg):
        try:
            event = json.loads(msg.data.decode())
            subject = msg.subject
            retries = msg.metadata.num_delivered if msg.metadata else 1

            if retries >= self._max_retries:
                logger.warning(f"Message retries exceeded for subject {subject}.")
                if self._dead_letter_subject:
                    await self._jetstream.publish(
                        subject=self._dead_letter_subject,
                        payload=msg.data,
                    )
                    logger.info(f"Message sent to dead-letter subject: {self._dead_letter_subject}")
                await msg.ack()
                return

            try:
                await self.callback(event, subject, retries)
                await msg.ack()
            except Exception as e:
                logger.error(f"Error in callback: {e}", exc_info=True)
                await msg.nak()
        except Exception as e:
            logger.error(f"Error in handle_message: {e}", exc_info=True)
            await msg.nak()

    async def stop(self):
        logger.info("Stopping consumer...")
        self._stop_event.set()

        logger.info("Waiting for internal queue to drain...")
        await self._message_queue.join()

        logger.info("Cancelling worker tasks...")
        for task in self._worker_tasks:
            task.cancel()
        await asyncio.gather(*self._worker_tasks, return_exceptions=True)

        try:
            logger.info("Draining NATS client...")
            await asyncio.wait_for(self._nats_client.drain(), timeout=10)
        except asyncio.TimeoutError:
            logger.error("Drain timeout exceeded.")
        except Exception as e:
            logger.error(f"Error during NATS drain: {e}")
        finally:
            await self._nats_client.close()
            logger.info("NATS connection closed.")

    def run(self):
        self.__loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.__loop)
        self.__loop.run_until_complete(self.start_task())
        self.__loop.close()
