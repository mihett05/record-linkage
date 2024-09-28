from collections import namedtuple

from standard import DatasetRow

from .pipelines import birthdates, emails, names, pipeline

Dataset1Row = namedtuple(
    "Dataset3Row",
    [
        "uid",
        "name",
        "email",
        "birthdate",
        "sex",
    ],
)

name_pipeline = pipeline(
    names.remove_newlines,
    names.detransliterate,
    names.split,
    names.remove_other_symbols,
    names.remove_lowercase,
    names.merge,
)

email_pipeline = pipeline(emails.remove_other_symbols, emails.slice_by_digits)
birthdate_pipeline = pipeline(
    birthdates.split,
    birthdates.make_year_to_4_digits,
    birthdates.merge,
)


def parse_dataset3_row(row: Dataset1Row) -> DatasetRow:
    uid, name, email, birthdate, sex = row
    return DatasetRow(
        uid,
        name_pipeline(name),
        email_pipeline(email),
        None,
        sex,
        birthdate_pipeline(birthdate),
        None,
        3,
    )
