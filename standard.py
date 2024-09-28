from collections import namedtuple

DatasetRow = namedtuple(
    "DatasetRow",
    [
        "uid",
        "full_name",
        "email",
        "address",
        "sex",
        "birthdate",
        "phone",
        "source",
    ],
)
