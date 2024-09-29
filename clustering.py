import pandas as pd
from uuid import UUID


class IncrementalStringRecordLinkage:
    def __init__(self):
        self.clusters: list[tuple[set[UUID], list[pd.Series]]] = []

    def add_record_pair(self, record_pair: tuple[pd.Series, pd.Series]):
        is_added = False
        first_item, second_item = record_pair

        for cluster_uids, cluster_items in self.clusters:
            if first_item['uid'] in cluster_uids:
                cluster_uids.add(second_item['uid'])
                cluster_items.append(second_item)
                is_added = True
                break
            elif second_item['uid'] in cluster_uids:
                cluster_uids.add(first_item['uid'])
                cluster_items.append(first_item)
                is_added = True
                break

        if not is_added:
            self.clusters.append(({first_item['uid'], second_item['uid']}, [first_item, second_item]))
        
    def add_record(self, record: pd.Series):
        self.clusters.append(({record['uid']}, [record]))
