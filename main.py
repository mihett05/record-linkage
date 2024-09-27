import asyncio

from datasource import ClickhouseClient
from preprocess.dataset1 import parse_dataset1_row


async def main():
    client = await ClickhouseClient.create()
    rows = await client.query("select * from table_dataset1 LIMIT 2")
    for row in rows:
        print(parse_dataset1_row(row))


if __name__ == "__main__":
    asyncio.run(main())
