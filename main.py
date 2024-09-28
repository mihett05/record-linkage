import asyncio

import pandas as pd

from clustering import IncrementalStringRecordLinkage
from comparison import classify, compare_rows
from datasource import ClickhouseClient
from preprocess import preprocess_table
from preprocess.dataset1 import parse_dataset1_row
from standard import DatasetRow


async def main():
    client = await ClickhouseClient.create()
    await client.create_normalized_table()
    await client.create_results_table()
    print("Starting preprocessing")
    await preprocess_table(client, "table_dataset1", parse_dataset1_row)
    # await preprocess_table(client, "table_dataset2", parse_dataset2_row)
    # await preprocess_table(client, "table_dataset2", parse_dataset3_row)
    print("Finished processing")

    rows = [DatasetRow(*row) for row in await client.query("select * from normalized LIMIT 100000")]

    dataframe = pd.DataFrame(rows)

    features = compare_rows(dataframe)

    matches = classify(features)

    incremental_cluster = IncrementalStringRecordLinkage()

    for match in matches:
        incremental_cluster.add_record((dataframe.iloc[match[0]], dataframe.iloc[match[1]]))

    for cluster_uids, cluster_items in incremental_cluster.clusters:
        await client.insert_result(list(cluster_uids), [], [])


if __name__ == "__main__":
    from time import time

    start_time = time()
    asyncio.run(main())
    print(f"time_taken: {time() - start_time}")
