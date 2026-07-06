from typing import List, Optional


class ConnectionParameters:
    def __init__(
        self,
        servers: List[str] = ['nats://localhost:4222'],
        username: Optional[str] = None,
        password: Optional[str] = None,
        token: Optional[str] = None,
        max_reconnect_attempts: int = 60,
        reconnect_time_wait: int = 2
    ):
        self.servers = servers
        self.username = username
        self.password = password
        self.token = token
        self.max_reconnect_attempts = max_reconnect_attempts
        self.reconnect_time_wait = reconnect_time_wait
