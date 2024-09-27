import clickhouse_connect
import clickhouse_connect.driver
import clickhouse_connect.driver.asyncclient
import clickhouse_connect.driver.binding
import clickhouse_connect.driver.query


class ClickhouseClient:
    def __init__(self, client: clickhouse_connect.driver.asyncclient.AsyncClient):
        self.client = client

    @classmethod
    async def create(cls) -> "ClickhouseClient":
        return ClickhouseClient(await cls.get_client())

    @staticmethod
    async def get_client() -> clickhouse_connect.driver.asyncclient.AsyncClient:
        return await clickhouse_connect.create_async_client(
            host="94.50.162.171", port=8123
        )

    async def query(self, query: str, parameters: dict | None = None) -> list:
        result = await self.client.query(query, parameters)
        return result.result_rows
