from collections import namedtuple

from .pipelines import addresses, birthdates, names, phones, pipeline
from ..standard import DatasetRow

Dataset2Row = namedtuple(
    "Dataset2Row",
    [
        "uid",
        "first_name",
        "middle_name",
        "last_name",
        "birthdate",
        "phone",
        "address",
    ],
)

name_pipeline = pipeline(
    names.merge,
    names.remove_newlines,
    names.detransliterate,
    names.split,
    names.remove_other_symbols,
    names.remove_lowercase,
    names.merge,
)

address_pipeline = pipeline(addresses.remove_new_lines, addresses.parse)
birthdate_pipeline = pipeline(
    birthdates.split,
    birthdates.make_year_to_4_digits,
    birthdates.merge,
)

phone_pipeline = pipeline(phones.parse_symbols, phones.remove_other_symbols)


def parse_dataset2_row(row: Dataset2Row) -> DatasetRow:
    uid, first_name, middle_name, last_name, birthdate, phone, address = row
    return DatasetRow(
        uid,
        name_pipeline([first_name, middle_name, last_name]),
        None,
        address_pipeline(address),
        None,
        birthdate_pipeline(birthdate),
        phone_pipeline(phone),
        2,
    )
