from nats_repository import ConnectionParameters


class TestConnectionParameters:
    def test_defaults(self):
        params = ConnectionParameters()

        assert params.servers == ['nats://localhost:4222']
        assert params.username is None
        assert params.password is None
        assert params.token is None
        assert params.max_reconnect_attempts == 60
        assert params.reconnect_time_wait == 2

    def test_username_password(self):
        params = ConnectionParameters(
            servers=['nats://host-a:4222', 'nats://host-b:4222'],
            username='user',
            password='pass',
        )

        assert params.servers == ['nats://host-a:4222', 'nats://host-b:4222']
        assert params.username == 'user'
        assert params.password == 'pass'
        assert params.token is None

    def test_token(self):
        params = ConnectionParameters(token='secret-token')

        assert params.token == 'secret-token'
        assert params.username is None
        assert params.password is None

    def test_reconnect_settings(self):
        params = ConnectionParameters(
            max_reconnect_attempts=5,
            reconnect_time_wait=10,
        )

        assert params.max_reconnect_attempts == 5
        assert params.reconnect_time_wait == 10
