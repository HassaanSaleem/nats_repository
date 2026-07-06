from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from nats_repository import AsyncIOPublisher, ConnectionParameters, Publisher


def make_publisher(**overrides):
    kwargs = {
        'connection_parameters': ConnectionParameters(),
        'stream_name': 'test_stream',
        'subjects': ['test.subject'],
    }
    kwargs.update(overrides)
    return AsyncIOPublisher(**kwargs)


class TestAsyncIOPublisherPublish:
    async def test_publish_calls_jetstream_with_subject_and_encoded_payload(self):
        publisher = make_publisher()
        publisher._jetstream = AsyncMock()

        await publisher.publish(payload='hello', subject='test.subject')

        publisher._jetstream.publish.assert_awaited_once_with(
            subject='test.subject',
            payload=b'hello',
        )
        assert publisher._acked_count == 1
        assert publisher._reject_count == 0

    async def test_publish_error_is_logged_and_not_raised(self, caplog):
        publisher = make_publisher()
        publisher._jetstream = AsyncMock()
        publisher._jetstream.publish.side_effect = Exception('publish failed')

        with caplog.at_level('ERROR'):
            await publisher.publish(payload='hello', subject='test.subject')

        assert publisher._acked_count == 0
        assert publisher._reject_count == 1
        assert any('Error publishing message' in record.message for record in caplog.records)

    async def test_publish_after_stop_raises(self):
        publisher = make_publisher()
        publisher._jetstream = AsyncMock()
        publisher._stop_event.set()

        with pytest.raises(Exception, match='Publisher is stopped'):
            await publisher.publish(payload='hello', subject='test.subject')

        publisher._jetstream.publish.assert_not_awaited()


class TestAsyncIOPublisherConnect:
    async def test_connect_passes_connection_parameters(self):
        params = ConnectionParameters(
            servers=['nats://host-a:4222'],
            username='user',
            password='pass',
            max_reconnect_attempts=7,
            reconnect_time_wait=3,
        )
        with patch('nats_repository.src.publisher.NATS') as nats_cls:
            client = MagicMock()
            client.is_connected = False
            client.connect = AsyncMock()
            client.jetstream.return_value = AsyncMock()
            nats_cls.return_value = client

            publisher = make_publisher(connection_parameters=params)
            await publisher.connect()

        client.connect.assert_awaited_once_with(
            servers=['nats://host-a:4222'],
            max_reconnect_attempts=7,
            reconnect_time_wait=3,
            user='user',
            password='pass',
        )

    async def test_connect_uses_token_when_no_credentials(self):
        params = ConnectionParameters(token='secret-token')
        with patch('nats_repository.src.publisher.NATS') as nats_cls:
            client = MagicMock()
            client.is_connected = False
            client.connect = AsyncMock()
            client.jetstream.return_value = AsyncMock()
            nats_cls.return_value = client

            publisher = make_publisher(connection_parameters=params)
            await publisher.connect()

        _, kwargs = client.connect.await_args
        assert kwargs['token'] == 'secret-token'
        assert 'user' not in kwargs
        assert 'password' not in kwargs


class TestAsyncIOPublisherStreamCreation:
    async def test_stream_created_when_missing(self):
        publisher = make_publisher(
            msg_retention='limits',
            msg_max_count=100_000,
            msg_max_size=2,
        )
        publisher._jetstream = AsyncMock()
        publisher._jetstream.stream_info.side_effect = Exception('stream not found')

        await publisher.create_stream_if_not_exists()

        publisher._jetstream.add_stream.assert_awaited_once()
        (config,), _ = publisher._jetstream.add_stream.await_args
        assert config.name == 'test_stream'
        assert config.subjects == ['test.subject']
        assert config.retention == 'limits'
        assert config.max_msgs == 100_000
        assert config.max_bytes == 2 * 1024 * 1024 * 1024

    async def test_stream_not_recreated_when_it_exists(self):
        publisher = make_publisher()
        publisher._jetstream = AsyncMock()

        await publisher.create_stream_if_not_exists()

        publisher._jetstream.add_stream.assert_not_awaited()


class TestPublisherBackpressure:
    def test_asyncio_publisher_queue_maxsize_is_four_times_concurrency(self):
        publisher = make_publisher(concurrency=25)

        assert publisher._max_queue_size == 100
        assert publisher._message_queue.maxsize == 100

    def test_multiprocessing_publisher_queue_size_is_four_times_concurrency(self):
        publisher = Publisher(
            connection_parameters=ConnectionParameters(),
            stream_name='test_stream',
            subjects=['test.subject'],
            concurrency=10,
        )

        assert publisher._max_queue_size == 40
