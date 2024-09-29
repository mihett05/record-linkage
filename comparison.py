import pandas as pd
import recordlinkage

from standard import DatasetRow


def compare_rows(rows: list[DatasetRow]) -> pd.DataFrame:
    original_dataframe = pd.DataFrame(rows)

    indexer = recordlinkage.Index()
    indexer.sortedneighbourhood("full_name", window=5)
    indexed = indexer.index(original_dataframe)

    comparer = recordlinkage.Compare()
    comparer.string("full_name", "full_name", method="levenshtein")
    comparer.string("email", "email", method="levenshtein")
    comparer.string("address", "address", method="levenshtein")
    comparer.exact("sex", "sex")
    comparer.string("birthdate", "birthdate", method="levenshtein")
    comparer.string("phone", "phone", method="levenshtein")

    return comparer.compute(indexed, original_dataframe)


def classify(features: pd.DataFrame) -> pd.Series:
    classifier = recordlinkage.ECMClassifier(binarize=0.9)
    return classifier.fit_predict(features)
