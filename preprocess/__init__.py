import asyncio
from math import ceil
from typing import Callable, TypeVar

from datasource import ClickhouseClient
from standard import DatasetRow

BATCH_SIZE = 100000

InputRow = TypeVar("InputRow")


async def preprocess_batch(client: ClickhouseClient, table_name: str, fn: Callable[[InputRow], DatasetRow], page: int):
    print(f"Batch {page} [{BATCH_SIZE * page} - {BATCH_SIZE * (page + 1)}]")
    rows = await client.query(f"SELECT * FROM {table_name} ORDER BY uid LIMIT {BATCH_SIZE} OFFSET {BATCH_SIZE * page}")
    print(f"Fetched batch {page}")
    processed_rows: list[DatasetRow] = [fn(row) for row in rows]
    print(f"Processed batch {page}")
    await client.insert_normalized(processed_rows)
    print(f"Inserted batch {page}")


async def preprocess_table(client: ClickhouseClient, table_name: str, fn: Callable[[InputRow], DatasetRow]):
    table_size = await client.count(table_name)
    tasks = []
    for page in range(ceil(table_size / BATCH_SIZE)):
        tasks.append(asyncio.create_task(preprocess_batch(client, table_name, fn, page)))
    print("Started batches")
    await asyncio.gather(*tasks)
