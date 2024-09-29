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
    # await client.create_normalized_table()
    # await client.create_results_table()
    # print("Starting preprocessing")
    # await preprocess_table(client, "table_dataset1", parse_dataset1_row)
    # await preprocess_table(client, "table_dataset2", parse_dataset2_row)
    # await preprocess_table(client, "table_dataset2", parse_dataset3_row)
    # print("Finished processing")

    rows = [DatasetRow(*row, source=1) for row in await client.query("select * from table_dataset1 LIMIT 1000000")]
    print('rows obtained')

    dataframe = pd.DataFrame(rows)

    features = compare_rows(dataframe)
    print('features obtained')

    matches = classify(features)
    print('matches obtained')

    incremental_cluster = IncrementalStringRecordLinkage()

    extra_indices = set(range(len(dataframe)))

    for match in matches:
        incremental_cluster.add_record_pair((dataframe.iloc[match[0]], dataframe.iloc[match[1]]))
        if match[0] in extra_indices:
            extra_indices.remove(match[0])
        if match[1] in extra_indices:
            extra_indices.remove(match[1])
    print('added pairs to clusters')
    
    for index in extra_indices:
        incremental_cluster.add_record((dataframe.iloc[index]))
    print('added extra indices to clusters')

    # total_length = 0
    # for cluster_uids, cluster_items in incremental_cluster.clusters:
    #     # print(cluster_items, end='\n\n')
    #     total_length += len(cluster_uids)

    # print(f'Total length: {total_length}')


if __name__ == "__main__":
    from time import time

    start_time = time()
    asyncio.run(main())
    print(f"time_taken: {time() - start_time}")
