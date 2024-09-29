import asyncio
from multiprocessing import Process

from datasource import ClickhouseClient
from preprocess import preprocess_table
from preprocess.dataset1 import parse_dataset1_row
from preprocess.dataset2 import parse_dataset2_row
from preprocess.dataset3 import parse_dataset3_row


async def run_preprocessing(table_name: str):
    client = await ClickhouseClient.create()
    match table_name:
        case "table_dataset1":
            fn = parse_dataset1_row
        case "table_dataset2":
            fn = parse_dataset2_row
        case "table_dataset3":
            fn = parse_dataset3_row
        case _:
            fn = parse_dataset1_row
    await client.create_normalized_table()
    await preprocess_table(client, table_name, fn)


def run_preprocess_for_table(table_name: str):
    loop = asyncio.new_event_loop()
    loop.run_until_complete(run_preprocessing(table_name))


def run_preprocess_for_all():
    tables = ["table_dataset1", "table_dataset2", "table_dataset3"]
    processes = [Process(target=run_preprocess_for_table, args=(table,)) for table in tables]
    for process in processes:
        process.start()
    for process in processes:
        process.join()
