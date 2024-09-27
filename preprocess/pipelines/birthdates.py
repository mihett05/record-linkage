from datetime import datetime


def split(birthdate: str) -> list[str]:
    if birthdate.startswith("-"):
        birthdate = birthdate[1:]
    return birthdate.split("-")


def make_year_to_4_digits(birthdate: list[str]) -> list[str]:
    year = birthdate[0]

    if len(year) == 4:
        return birthdate
    if len(year) == 3:
        return ["1" + "".join(sorted(year, reverse=True))] + birthdate[1:]
    if len(year) == 2:
        year20 = int("20" + year)
        year19 = int("19" + year)
        if year20 > datetime.now().year:
            return [str(year19)] + birthdate[1:]
        return [str(year20)] + birthdate[1:]
    if len(year) == 1:
        return [f"200{year}"] + birthdate[1:]
    return birthdate


def merge(birthdate: list[str]) -> str:
    return "-".join(birthdate)
