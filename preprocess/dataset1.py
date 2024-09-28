from collections import namedtuple

from .pipelines import addresses, birthdates, emails, names, phones, pipeline

Dataset1Row = namedtuple('Dataset1Row', ['uid', 'name', 'email', 'address', 'sex', 'birthdate', 'phone'])

name_pipeline = pipeline(
    names.remove_newlines,
    names.detransliterate,
    names.split,
    names.remove_other_symbols,
    names.remove_lowercase,
    names.merge,
)

email_pipeline = pipeline(emails.remove_other_symbols, emails.slice_by_digits)
address_pipeline = pipeline(addresses.remove_new_lines, addresses.parse)
birthdate_pipeline = pipeline(
    birthdates.split,
    birthdates.make_year_to_4_digits,
    birthdates.merge,
)

phone_pipeline = pipeline(phones.parse_symbols, phones.remove_other_symbols)


def parse_dataset1_row(row: Dataset1Row) -> Dataset1Row:
    uid, name, email, address, sex, birthdate, phone = row
    return Dataset1Row(
        uid,
        name_pipeline(name),
        email_pipeline(email),
        address_pipeline(address),
        sex,
        birthdate_pipeline(birthdate),
        phone_pipeline(phone),
    )
