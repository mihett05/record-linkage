from datasource import ClickhouseClient

from .column import columns_for_group, group_query


async def get_list_of_groups_by_columns(client: ClickhouseClient):
    columns_groups = {}
    for column in columns_for_group:
        columns_groups[column] = await group_query(client, column)
    return columns_groups
