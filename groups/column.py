from datasource import ClickhouseClient

columns = [
    "uid",
    "full_name",
    "email",
    "address",
    "sex",
    "birthdate",
    "phone",
    "source",
]

columns_for_group = [
    "full_name",
    "email",
    "address",
    "birthdate",
    "phone",
]


async def group_query(client: ClickhouseClient, column: str):
    query_columns = [col for col in columns if col != column]
    aggregated = ", ".join([f"groupArray({col}) {col}" for col in query_columns])
    query = f"""
    select * from (
        select {column}, {aggregated}, count(*) as c
        from normalized group by {column}
    ) where c > 2
    """
    groups = [list(group[:-1]) for group in await client.query(query)]
    entity_groups = []
    for group in groups:
        target_column = group[0]
        group[1:] = [column for column in group[1:]]
        aggregated_rows = list(zip(*group[1:]))
        entities = []
        for row in aggregated_rows:
            entity = dict(list(zip(query_columns, row)))
            entities.append(entity)
        entity_groups.append(
            {
                "column": column,
                "column_value": target_column,
                "entities": entities,
            }
        )
    return entity_groups
