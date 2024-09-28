import asyncio
from math import log
from uuid import UUID

import pandas as pd

from comparison import classify, compare_rows
from clustering import IncrementalStringRecordLinkage
from datasource import ClickhouseClient
from preprocess import preprocess_table
from preprocess.dataset1 import Dataset1Row, parse_dataset1_row
from preprocess.dataset2 import parse_dataset2_row
from preprocess.dataset3 import parse_dataset3_row

fake_data = [
    Dataset1Row(
        uid=UUID("e13cf60d-b63a-4d93-8014-1468aa87aae1"),
        full_name="БУЛАН БАААААМАН ЧОРИЕВИЧ",
        email="bamanbvbdulan2712671",
        address="Злавпатоуст,Высотный,6/5,537,005088",
        sex="m",
        birthdate="2972-11-24",
        phone="85589856357",
    ),
    Dataset1Row(
        uid=UUID("b0a915e4-b257-4328-8014-144f2a92fd25"),
        full_name="МУППЗЫКАНТОВА ТАИСИЯ ЧИНГ",
        email="taisijambvuzykantova5522165",
        address=",Запорожский,23,4,417936",
        sex="f",
        birthdate="1991-12-11",
        phone="85202803460",
    ),
    Dataset1Row(
        uid=UUID("aad6c6a4-832c-4d27-8014-11de72ff874a"),
        full_name="БАЙДАВЛЕТОВ МАХКАМДЖОН ВАЛИЕВИЧ",
        email="mahkamdzhonbajdavletov5152570",
        address=",Смирнова,92,3,044111",
        sex="m",
        birthdate="2001-05-04",
        phone="88991724737",
    ),
    Dataset1Row(
        uid=UUID("e13cf60d-4d93-4d93-8014-1468aa87aae1"),
        full_name="БУЛАН БАМАН ЧООРИЕВИЧ",
        email="bamanbukjjkuulan2712671",
        address="Златоуст,Высотный,6/б,537,005088",
        sex="m",
        birthdate="2972-11-24",
        phone="85589856357",
    ),
    Dataset1Row(
        uid=UUID("b0a915e4-4d93-4328-8014-144f2a92fd25"),
        full_name="МУЗЫКАНТОВА ТАИСИЯ ТАИСИЯ ЧИНГ",
        email="taisijamuzykantova5522165",
        address=",Запорожский,23,4,417936",
        sex="f",
        birthdate="1991-12-11",
        phone="85202803460",
    ),
    Dataset1Row(
        uid=UUID("aad6c6a4-b63a-4d27-8014-11de72ff874a"),
        full_name="БАЙАФЫВАДАВЛЕТОВ МАХКАМДЖОН ВАЛИЕВИЧ",
        email="mahkamdzhdfsonbajdavletov5152570",
        address=",Смирнова,92,3,044111",
        sex="m",
        birthdate="2001-05-04",
        phone="88991724737",
    ),
]


async def main():
    client = await ClickhouseClient.create()
    await client.create_normalized_table()
    await client.create_results_table()
    print("Starting preprocessing")
    await preprocess_table(client, "table_dataset1", parse_dataset1_row)
    await preprocess_table(client, "table_dataset2", parse_dataset2_row)
    await preprocess_table(client, "table_dataset2", parse_dataset3_row)
    print("Finished processing")
    # groups = await get_list_of_groups_by_columns(client)
    # for column in groups:
    #     print(column)
    # TODO: поиск по нормализованной таблице
    # rows = await client.query("select * from table_dataset1 LIMIT 100000")
    # parsed_rows = list(map(parse_dataset1_row, rows))

    # dataframe = pd.DataFrame(parsed_rows)
    
    # features = compare_rows(dataframe)

    # matches = classify(features)

    # incremental_cluster = IncrementalStringRecordLinkage()

    # for match in matches:
    #     incremental_cluster.add_record((dataframe.iloc[match[0]], dataframe.iloc[match[1]]))
    
    # for cluster_uids, cluster_items in incremental_cluster.clusters:
    #     print(cluster_items, end='\n\n')
    # print("Starting preprocessing")
    # await preprocess_table(client, "table_dataset1", parse_dataset1_row)
    # print("Finished")


def test_comparasion():
    dataframe = pd.DataFrame(fake_data)

    features = compare_rows(dataframe)

    matches = classify(features)
    print(matches)



if __name__ == "__main__":
    from time import time

    start_time = time()
    asyncio.run(main())
    # test_comparasion()
    print(f"time_taken: {time() - start_time}")
