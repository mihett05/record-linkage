import asyncio
import sys
from math import ceil

from clustering import IncrementalStringRecordLinkage
from comparison import classify, compare_rows
from datasource import ClickhouseClient

PAGE_SIZE = 100000


async def run_deduplicating(client: ClickhouseClient, page: int, table: str):
    t = time()
    dataframe = await client.query_df(f"select * from {table} order by full_name limit {PAGE_SIZE} offset {page * PAGE_SIZE}")
    print(f"rows obtained {page}")

    features = compare_rows(dataframe)
    print(f"features obtained {page}")

    matches = classify(features)
    print(f"matches obtained {page}")

    incremental_cluster = IncrementalStringRecordLinkage()

    extra_indices = set(range(len(dataframe)))

    for match in matches:
        incremental_cluster.add_record_pair((dataframe.iloc[match[0]], dataframe.iloc[match[1]]))
        if match[0] in extra_indices:
            extra_indices.remove(match[0])
        if match[1] in extra_indices:
            extra_indices.remove(match[1])
    print(f"added pairs to clusters {page}")

    print(f"Inserting {page}")
    results = []
    if table.startswith("normalized"):
        for cluser_uids, cluster_items in incremental_cluster.clusters:
            uids_and_source = [(item[0], item[-1]) for item in cluster_items]
            uids = {1: [], 2: [], 3: []}
            for uid, source in uids_and_source:
                uids[source].append(uid)
            results.append(list(uids.values()))
    else:
        for cluser_uids, cluster_items in incremental_cluster.clusters:
            results.append([cluser_uids, [], []])
    asyncio.create_task(client.insert_result_rows(results))
    print(f"Page [{page}] for {time() - t}s")


async def main():
    client = await ClickhouseClient.create()
    await client.create_normalized_table()
    await client.create_results_table()
    print("Starting preprocessing")
    # await preprocess_table(client, "table_dataset1", parse_dataset1_row)
    # await preprocess_table(client, "table_dataset2", parse_dataset2_row)
    # await preprocess_table(client, "table_dataset2", parse_dataset3_row)
    print("Finished processing")

    for table in ["table_dataset1", "table_dataset2", "table_dataset3"]:
        count = await client.count(table)  # normalized
        for page in range(ceil(count / PAGE_SIZE)):
            await run_deduplicating(client, page, table)  # normalized


if __name__ == "__main__":
    from time import time

    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    start_time = time()
    asyncio.run(main())
    print(f"time_taken: {time() - start_time}")
