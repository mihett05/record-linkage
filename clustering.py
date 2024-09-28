import pandas as pd
from uuid import UUID


class IncrementalStringRecordLinkage:
    def __init__(self):
        self.clusters: list[tuple[set[UUID], list[pd.Series]]] = []

    def add_record(self, record_pair: tuple[pd.Series, pd.Series]):
        is_added = False
        first_item, second_item = record_pair

        for cluster_uids, cluster_items in self.clusters:
            if first_item['uid'] in cluster_uids:
                cluster_uids.add(second_item['uid'])
                cluster_items.append(second_item)
                is_added = True
            elif second_item['uid'] in cluster_uids:
                cluster_uids.add(first_item['uid'])
                cluster_items.append(first_item)
                is_added = True

        if not is_added:
            self.clusters.append(({first_item['uid'], second_item['uid']}, [first_item, second_item]))

    def add_records(self, new_records: list[tuple[pd.Series, pd.Series]]):
        for record in new_records:
            self.add_record(record)

    def get_clusters(self):
        return self.clusters
