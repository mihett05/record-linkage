from functools import reduce

transliteration = {
    "Ts": "Ц",
    "Ch": "Ч",
    "Sh": "Ш",
    "Shch": "Щ",
    "Yu": "Ю",
    "Iu": "Ю",
    "Ju": "Ю",
    "Ya": "Я",
    "Ia": "Я",
    "Ja": "Я",
    "Kh": "Х",
    "Ye": "Е",
    "Zh": "Ж",
    "A": "А",
    "B": "Б",
    "V": "В",
    "G": "Г",
    "D": "Д",
    "E": "Е",
    "Z": "З",
    "I": "И",
    "Y": "Й",
    "K": "К",
    "L": "Л",
    "M": "М",
    "N": "Н",
    "O": "О",
    "P": "П",
    "R": "Р",
    "S": "С",
    "T": "Т",
    "U": "У",
    "F": "Ф",
    "H": "Х",
    "'": "Ь",
    "h": "Ш",  # Sh
}


def remove_newlines(name: str) -> str:
    return name.replace("\n", "").replace("\t", "")


def detransliterate(name: str) -> str:
    return reduce(
        lambda prev, curr: prev.replace(curr[0], curr[1]),
        transliteration.items(),
        name,
    )


def split(name: str) -> list[str]:
    return name.split()


def remove_other_symbols(name: list[str]) -> list[str]:
    return ["".join([c for c in part if c.isalpha()]) for part in name]


def remove_lowercase(name: list[str]) -> list[str]:
    return [part for part in name if part.isupper()]


def merge(name: list[str]) -> str:
    return " ".join(name)
