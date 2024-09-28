import asyncio
from itertools import combinations
from math import log
from uuid import UUID

import pandas as pd
from nltk.metrics import distance

from datasource import ClickhouseClient
from preprocess import preprocess_table
from preprocess.dataset1 import Dataset1Row, parse_dataset1_row

fake_data = [
    Dataset1Row(
        uid=UUID("e13cf60d-b63a-4d93-8014-1468aa87aae1"),
        full_name="БУЛАН БАМАН ЧОРИЕВИЧ",
        email="bamanbulan2712671",
        address="Златоуст,Высотный,6/5,537,005088",
        sex="m",
        birthdate="2972-11-24",
        phone="85589856357",
    ),
    Dataset1Row(
        uid=UUID("b0a915e4-b257-4328-8014-144f2a92fd25"),
        full_name="МУЗЫКАНТОВА ТАИСИЯ ЧИНГ",
        email="taisijamuzykantova5522165",
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
        email="bamanbuuulan2712671",
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
        full_name="БАЙДАВЛЕТОВ МАХКАМДЖОН ВАЛИЕВИЧ",
        email="mahkamdzhonbajdavletov5152570",
        address=",Смирнова,92,3,044111",
        sex="m",
        birthdate="2001-05-04",
        phone="88991724737",
    ),
]


async def main():
    client = await ClickhouseClient.create()
    # rows = await client.query("select * from table_dataset1 LIMIT 1000")
    # parsed_rows = list(map(parse_dataset1_row, rows))
    # print(*parsed_rows, sep='\n')
    await client.create_normalized_table()
    print("Starting preprocessing")
    await preprocess_table(client, "table_dataset1", parse_dataset1_row)
    print("Finished")


def test_comparasion():
    dataframe = pd.DataFrame(fake_data)

    comparison_vectors = []

    for first_index, second_index in combinations(dataframe.index, r=2):
        first_record = dataframe.loc[first_index]
        second_record = dataframe.loc[second_index]

        comparison_vectors.append(
            {
                "uid1": first_record.uid,
                "uid2": second_record.uid,
                "name1": first_record["name"],
                "name2": second_record["name"],
                "name_comparison": distance.jaro_winkler_similarity(first_record["name"], second_record["name"]),
                "email_comparison": distance.jaro_winkler_similarity(first_record.email, second_record.email),
                "address_comparison": distance.jaro_winkler_similarity(first_record.address, second_record.address),
                "sex_comparison": distance.jaro_winkler_similarity(first_record.sex, second_record.sex),
                "birthdate_comparison": distance.jaro_winkler_similarity(first_record.birthdate, second_record.birthdate),
                "phone_comparison": distance.jaro_winkler_similarity(first_record.phone, second_record.phone),
            }
        )

    comparison_dataframe = pd.DataFrame(comparison_vectors)
    comparison_dataframe["weight"] = comparison_dataframe.apply(calculate_weight, axis=1)
    comparison_dataframe["classification"] = comparison_dataframe["weight"].apply(classify_pair)
    print(comparison_dataframe.where(comparison_dataframe["classification"] == "Match").dropna())

    comparison_dataframe[["uid1", "uid2", "name1", "name2", "weight", "classification"]].to_csv("res.csv")


m_probs = {
    "name_comparison": 0.9,
    "email_comparison": 0.9,
    "address_comparison": 0.9,
    "sex_comparison": 0.9,
    "birthdate_comparison": 0.9,
    "phone_comparison": 0.9,
}

u_probs = {
    "name_comparison": 0.4,
    "email_comparison": 0.4,
    "address_comparison": 0.4,
    "sex_comparison": 0.4,
    "birthdate_comparison": 0.4,
    "phone_comparison": 0.4,
}


def calculate_weight(row):
    total_weight = 0
    for attr in [
        "name_comparison",
        "email_comparison",
        "address_comparison",
        "sex_comparison",
        "birthdate_comparison",
        "phone_comparison",
    ]:
        m = m_probs[attr]
        u = u_probs[attr]
        if row[attr] >= 0.8:
            weight = log(m / u)
        else:
            weight = log((1 - m) / (1 - u))
        total_weight += weight
    return total_weight


def classify_pair(weight):
    upper_threshold, lower_threshold = 2.0, -2.0

    if weight >= upper_threshold:
        return "Match"
    elif weight <= lower_threshold:
        return "Non-Match"
    else:
        return "Possible Match"


if __name__ == "__main__":
    from time import time

    start_time = time()
    asyncio.run(main())
    print(f"time_taken: {time() - start_time}")
