import recordlinkage
from standard import DatasetRow
import pandas as pd


def compare_rows(rows: list[DatasetRow]) -> pd.DataFrame:
    original_dataframe = pd.DataFrame(rows)

    indexer = recordlinkage.Index()
    indexer.block('full_name')
    indexer.block('email')
    indexed = indexer.index(original_dataframe)

    comparer = recordlinkage.Compare()
    comparer.string('full_name', 'full_name', method='jarowinkler')
    comparer.string('email', 'email', method='jarowinkler')
    comparer.string('address', 'address', method='jarowinkler')
    comparer.exact('sex', 'sex')
    comparer.string('birthdate', 'birthdate', method='jarowinkler')
    comparer.string('phone', 'phone', method='jarowinkler')

    return comparer.compute(indexed, original_dataframe)


def classify(features: pd.DataFrame) -> pd.Series:
    classifier = recordlinkage.ECMClassifier(binarize=0.95)
    return classifier.fit_predict(features)
