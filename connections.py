import clickhouse_connect
from typing import Optional

class ClickHouseConnection:
    _client = None

    @classmethod
    def get_client(cls, host: str = "localhost", port: int = 8123, user: str = "default", password: Optional[str] = None):
        if cls._client is None:
            cls._client = clickhouse_connect.get_client(host=host, port=port, username=user, password="")
        return cls._client

    @classmethod
    def close(cls):
        if cls._client:
            cls._client.close()
            cls._client = None
