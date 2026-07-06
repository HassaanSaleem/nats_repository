import asyncio
import json
from unittest.mock import AsyncMock, MagicMock

from nats.js.api import AckPolicy, DeliverPolicy

from nats_repository import ConnectionParameters, Consumer


def make_consumer(**overrides):
    kwargs = {
        'connection_parameters': ConnectionParameters(),
        'stream_name': 'test_stream',
        'subjects': ['orders.created'],
    }
    kwargs.update(overrides)
    return Consumer(**kwargs)


def make_message(payload=None, subject='orders.created', num_delivered=1):
    msg = MagicMock()
    msg.data = payload if payload is not None else json.dumps({'id': 1}).encode()
    msg.subject = subject
    msg.metadata.num_delivered = num_delivered
    msg.ack = AsyncMock()
    msg.nak = AsyncMock()
    return msg


class TestConsumerBackpressure:
    def test_queue_maxsize_is_four_times_concurrency(self):
        consumer = make_consumer(concurrency=10)

        assert consumer.max_queue_size == 40
        assert consumer._message_queue.maxsize == 40

    def test_no_unused_semaphore(self):
        consumer = make_consumer()

        assert not hasattr(consumer, '_semaphore')


class TestConsumerSubscribe:
    async def test_creates_durable_consumer_per_subject(self):
        consumer = make_consumer(
            subjects=['orders.created'],
            max_retries=3,
            backoff_intervals=[1, 5, 10],
        )
        consumer._jetstream = AsyncMock()
        consumer._jetstream.consumer_info.side_effect = Exception('consumer not found')
        consumer._stop_event.set()

        await consumer.subscribe()
        await asyncio.sleep(0)

        consumer._jetstream.add_consumer.assert_awaited_once()
        _, kwargs = consumer._jetstream.add_consumer.await_args
        config = kwargs['config']
        assert kwargs['stream'] == 'test_stream'
        assert config.durable_name == 'orders_created_durable'
        assert config.deliver_policy == DeliverPolicy.ALL
        assert config.ack_policy == AckPolicy.EXPLICIT
        assert config.max_deliver == 3
        assert config.backoff == [1, 5, 10]
        assert config.filter_subject == 'orders.created'

        consumer._jetstream.pull_subscribe.assert_awaited_once_with(
            subject='orders.created',
            durable='orders_created_durable',
        )

    async def test_existing_consumer_is_not_recreated(self):
        consumer = make_consumer(subjects=['orders.created'])
        consumer._jetstream = AsyncMock()
        consumer._stop_event.set()

        await consumer.subscribe()
        await asyncio.sleep(0)

        consumer._jetstream.add_consumer.assert_not_awaited()
        consumer._jetstream.pull_subscribe.assert_awaited_once()

    async def test_wildcard_characters_are_sanitized_in_durable_name(self):
        consumer = make_consumer(subjects=['orders.*'])
        consumer._jetstream = AsyncMock()
        consumer._jetstream.consumer_info.side_effect = Exception('consumer not found')
        consumer._stop_event.set()

        await consumer.subscribe()
        await asyncio.sleep(0)

        _, kwargs = consumer._jetstream.add_consumer.await_args
        assert kwargs['config'].durable_name == 'orders___durable'


class TestConsumerStreamCreation:
    async def test_stream_created_when_missing(self):
        consumer = make_consumer(
            msg_retention='limits',
            msg_max_count=100_000,
            msg_max_size=1,
        )
        consumer._jetstream = AsyncMock()
        consumer._jetstream.stream_info.side_effect = Exception('stream not found')

        await consumer.create_stream_if_not_exists()

        consumer._jetstream.add_stream.assert_awaited_once()
        (config,), _ = consumer._jetstream.add_stream.await_args
        assert config.name == 'test_stream'
        assert config.subjects == ['orders.created']
        assert config.retention == 'limits'
        assert config.max_msgs == 100_000
        assert config.max_bytes == 1024 * 1024 * 1024

    async def test_dead_letter_stream_created_when_configured(self):
        consumer = make_consumer(dead_letter_subject='orders_dlq')
        consumer._jetstream = AsyncMock()
        consumer._jetstream.stream_info.side_effect = Exception('stream not found')

        await consumer.create_stream_if_not_exists()

        assert consumer._jetstream.add_stream.await_count == 2
        (dlq_config,), _ = consumer._jetstream.add_stream.await_args_list[1]
        assert dlq_config.name == 'orders_dlq'
        assert dlq_config.subjects == ['orders_dlq']

    async def test_no_dead_letter_stream_without_subject(self):
        consumer = make_consumer()
        consumer._jetstream = AsyncMock()

        await consumer.create_stream_if_not_exists()

        consumer._jetstream.add_stream.assert_not_awaited()
        assert consumer._jetstream.stream_info.await_count == 1


class TestConsumerHandleMessage:
    async def test_successful_callback_acks_message(self):
        consumer = make_consumer(max_retries=5)
        consumer._jetstream = AsyncMock()
        consumer.callback = AsyncMock()
        msg = make_message(num_delivered=1)

        await consumer.handle_message(msg)

        consumer.callback.assert_awaited_once_with({'id': 1}, 'orders.created', 1)
        msg.ack.assert_awaited_once()
        msg.nak.assert_not_awaited()

    async def test_failing_callback_naks_message(self):
        consumer = make_consumer(max_retries=5)
        consumer._jetstream = AsyncMock()
        consumer.callback = AsyncMock(side_effect=RuntimeError('handler failed'))
        msg = make_message(num_delivered=2)

        await consumer.handle_message(msg)

        msg.nak.assert_awaited_once()
        msg.ack.assert_not_awaited()

    async def test_retries_exhausted_publishes_to_dead_letter_and_acks(self):
        consumer = make_consumer(max_retries=3, dead_letter_subject='orders_dlq')
        consumer._jetstream = AsyncMock()
        consumer.callback = AsyncMock()
        msg = make_message(num_delivered=3)

        await consumer.handle_message(msg)

        consumer._jetstream.publish.assert_awaited_once_with(
            subject='orders_dlq',
            payload=msg.data,
        )
        msg.ack.assert_awaited_once()
        msg.nak.assert_not_awaited()
        consumer.callback.assert_not_awaited()

    async def test_retries_exhausted_without_dead_letter_acks_only(self):
        consumer = make_consumer(max_retries=3)
        consumer._jetstream = AsyncMock()
        consumer.callback = AsyncMock()
        msg = make_message(num_delivered=3)

        await consumer.handle_message(msg)

        consumer._jetstream.publish.assert_not_awaited()
        msg.ack.assert_awaited_once()
        consumer.callback.assert_not_awaited()

    async def test_undecodable_payload_naks_message(self):
        consumer = make_consumer()
        consumer._jetstream = AsyncMock()
        consumer.callback = AsyncMock()
        msg = make_message(payload=b'not json')

        await consumer.handle_message(msg)

        msg.nak.assert_awaited_once()
        msg.ack.assert_not_awaited()
        consumer.callback.assert_not_awaited()
