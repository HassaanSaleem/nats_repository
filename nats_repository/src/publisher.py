from nats_repository.src.connection_parameters import ConnectionParameters
from multiprocessing import Process, Queue, Event, current_process
from nats.aio.client import Client as NATS
from nats.js.api import StreamConfig
from typing import List
import threading
import asyncio
import logging
import signal
import queue
import time

logger = logging.getLogger(__name__)


def is_main_process() -> bool:
    return (
        current_process().name == "MainProcess"
        and threading.current_thread() is threading.main_thread()
    )


class Publisher(Process):
    """
    A wrapper class that runs AsyncIOPublisher in a subprocess and communicates
    using multiprocessing.Queue.
    """

    def __init__(
        self,
        connection_parameters: ConnectionParameters,
        stream_name: str,
        subjects: List[str],
        max_retries: int = 5,
        backoff_intervals: List[float] = None,
        concurrency: int = 100,
        msg_retention: str = 'workqueue',
        msg_max_count: int = 1_000_000_000,
        msg_max_size: int = 1
    ):
        super().__init__()

        self._connection_parameters = connection_parameters
        self._stream_name = stream_name
        self._subjects = subjects
        self._concurrency = int(concurrency)
        self._max_queue_size = self._concurrency * 4
        self._msg_retention = msg_retention
        self._msg_max_count = int(msg_max_count)
        self._msg_max_size = int(msg_max_size)
        self._backoff_intervals = backoff_intervals
        self._max_retries = max_retries

        self._task_queue = Queue(maxsize=self._max_queue_size)
        self._stop_flag = Queue(maxsize=1)
        self._started = Event()

    def run(self):
        """
        Entry point for the subprocess.
        """
        asyncio.run(self._run_async_publisher())

    async def _run_async_publisher(self):
        """
        Run the AsyncIOPublisher and handle incoming tasks.
        """
        publisher = AsyncIOPublisher(
            connection_parameters=self._connection_parameters,
            stream_name=self._stream_name,
            subjects=self._subjects,
            concurrency=self._concurrency,
            msg_retention=self._msg_retention,
            msg_max_count=self._msg_max_count,
            msg_max_size=self._msg_max_size,
            max_retries=self._max_retries,
            backoff_intervals=self._backoff_intervals
        )
        await publisher.start()

        # Signal that the publisher is ready.
        self._started.set()
        try:
            while self._stop_flag.empty() or not self._task_queue.empty():
                try:
                    payload, subject = await asyncio.to_thread(self._task_queue.get, 0.1)
                    await publisher.publish(payload, subject)
                except queue.Empty:
                    await asyncio.sleep(0.1)
                    continue
                except Exception as e:
                    logger.error(f"Error processing task: {e}", exc_info=True)
        except asyncio.CancelledError:
            logger.info("Shutdown signal received in subprocess.")
        finally:
            await publisher.stop()

    def publish(self, payload: str, subject: str):
        """
        Add a task to the queue for publishing.
        """
        if not self._stop_flag.empty():
            raise Exception("Publisher is stopped. Can't publish.")
        self._task_queue.put((payload, subject))

    def stop(self):
        """
        Signal the subprocess to stop and wait for the task queue to be empty.
        """
        self._stop_flag.put(True)
        while not self._task_queue.empty():
            time.sleep(0.1)

        self.join()


class AsyncIOPublisher:
    """
    A publisher for sending messages to a NATS JetStream.

    Parameters:
        connection_parameters (ConnectionParameters):
            The connection details for the NATS server (e.g., servers, authentication).
        stream_name (str):
            The name of the JetStream to publish messages to.
        subjects (List[str]):
            A list of subjects to publish messages on.
        concurrency (int, optional):
            The maximum number of concurrent publishing tasks. Default is 100.
        msg_retention (str, optional):
            The message retention policy. Options are 'limits', 'interest', or 'workqueue'.
            Default is 'workqueue'.
        msg_max_count (int, optional):
            The maximum number of messages to retain in the stream. Default is 1,000,000,000.
        msg_max_size (int, optional):
            The maximum size of the stream in **GB**. Default is 100.

    Example:
        publisher = Publisher(
            connection_parameters=ConnectionParameters(...),
            stream_name="example_stream",
            subjects=["example.subject"],
            msg_max_size=1,  # 1 GB
            msg_max_count=1_000_000_000   # 1B
        )
        await publisher.publish(payload="data", subject="example.subject")
    """

    def __init__(
        self,
        connection_parameters: ConnectionParameters,
        stream_name: str,
        subjects: List[str],
        max_retries: int = 5,
        backoff_intervals: List[float] = None,
        concurrency: int = 100,
        msg_retention: str = 'workqueue',
        msg_max_count: int = 1_000_000_000,
        msg_max_size: int = 1
    ):
        self._connection_parameters = connection_parameters
        self._stream_name = stream_name
        self._subjects = subjects
        self._max_retries = max_retries
        self._backoff_intervals = backoff_intervals
        self._nats_client: NATS = NATS()
        self._jetstream = None
        self._acked_count = 0
        self._reject_count = 0
        self._concurrency = int(concurrency)
        self._max_queue_size = self._concurrency * 4
        self._worker_task = None
        self._msg_retention = msg_retention
        self._msg_max_count = int(msg_max_count)
        self._msg_max_size = int(msg_max_size * 1024 * 1024 * 1024)
        self._semaphore = asyncio.Semaphore(self._concurrency)
        self._message_queue = asyncio.Queue(maxsize=self._max_queue_size)
        self._stop_event = asyncio.Event()

    async def start(self):
        if is_main_process():
            self._register_signal_handlers()
        await self.connect()
        self._worker_task = asyncio.create_task(self._message_worker())

    def _register_signal_handlers(self):
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(self._shutdown(s)))

    async def _shutdown(self, sig):
        logger.info(f"Received exit signal {sig.name}...")
        self._stop_event.set()
        await self.stop()

    async def publish(self, payload: str, subject: str):
        if not self._stop_event.is_set():
            try:
                await self._jetstream.publish(
                    subject=subject,
                    payload=payload.encode('utf-8')
                )
                self._acked_count += 1
                logger.info(f"Published message count: {self._acked_count}.")
            except Exception as e:
                self._reject_count += 1
                logger.error(f"Error publishing message: {e}", exc_info=True)
        else:
            raise Exception("Publisher is stopped.")

    async def stop(self):
        self._stop_event.set()
        if self._worker_task:
            await self._worker_task
        await self.close()

    async def connect(self):
        if self._nats_client.is_connected:
            return
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
            try:
                await self._jetstream.add_stream(stream_config)
                logger.info(f"Stream {self._stream_name} created.")
            except Exception as e:
                logger.error(f"Error creating stream: {e}")
                raise e

    async def _message_worker(self):
        while not self._stop_event.is_set():
            await asyncio.sleep(0.1)

    async def close(self):
        if self._nats_client.is_connected:
            await self._nats_client.close()
            logger.info("NATS connection closed.")
