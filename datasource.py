from uuid import UUID

import clickhouse_connect
import clickhouse_connect.driver
import clickhouse_connect.driver.asyncclient
import clickhouse_connect.driver.binding
import clickhouse_connect.driver.query

from standard import DatasetRow


class ClickhouseClient:
    def __init__(self, client: clickhouse_connect.driver.asyncclient.AsyncClient):
        self.client = client

    @classmethod
    async def create(cls) -> "ClickhouseClient":
        return ClickhouseClient(await cls.get_client())

    @staticmethod
    async def get_client() -> clickhouse_connect.driver.asyncclient.AsyncClient:
        return await clickhouse_connect.create_async_client(host="94.50.162.171", port=8123)

    async def create_normalized_table(self):
        await self.client.command("""
            create table if not exists normalized
            (
                uid UUID,
                full_name String,
                email Nullable(String),
                address Nullable(String),
                sex Nullable(String),
                birthdate String,
                phone Nullable(String),
                source Int8
            )
            engine = MergeTree()
            partition by murmurHash3_32(uid) % 8
            order by uid;
        """)

    async def create_results_table(self):
        await self.client.command("""
        CREATE TABLE IF NOT EXISTS table_results (
            id_is1 Array(UUID),
            id_is2 Array(UUID),
            id_is3 Array(UUID)
        ) ENGINE = MergeTree()
        ORDER BY id_is1
        """)

    async def count(self, table_name: str) -> int:
        return (await self.query(f"select count(*) from {table_name}"))[0][0]

    async def query(self, query: str, parameters: dict | None = None) -> list:
        result = await self.client.query(query, parameters)
        return result.result_rows

    async def insert_normalized(self, row: list[DatasetRow]):
        await self.client.insert("normalized", row)

    async def insert_result(self, ids1: list[str | UUID], ids2: list[str | UUID], ids3: list[str | UUID]):
        await self.client.insert(
            "table_results",
            [[uid if isinstance(uid, UUID) else UUID(uid) for uid in ids_list] for ids_list in [ids1, ids2, ids3]],
        )
