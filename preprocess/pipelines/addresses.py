from dataclasses import dataclass


@dataclass
class Address:
    city: str = ""
    street: str = ""
    house: str = ""
    flat: str = ""
    index: str = ""


def remove_new_lines(address: str) -> str:
    return address.replace("\n", "")


def parse(address: str) -> str:
    addr = Address()
    for part in address.strip().split(","):
        part = part.strip()
        if " " in part:
            prefix, suffix = part.split(maxsplit=1)
            if (
                "д." in prefix
                and (suffix.count(" ") >= 2 or not addr.house)
                and len([c for c in suffix if c.isdigit()]) > 0
            ) or "Дом" in prefix:
                if suffix.count(" ") >= 2:
                    house, flat_prefix, flat = suffix.split(maxsplit=2)
                    addr.house = house
                    if " " in flat:
                        flat, index = flat.split(maxsplit=1)
                        addr.flat = flat
                        addr.index = index
                    else:
                        addr.flat = flat
                else:
                    addr.house = suffix
            elif prefix.strip() in [
                "г.",
                "Город",
                "клх",
                "Колхоз",
                "с.",
                "сю",
                "Село",
                "к.",
                "ст.",
                "д.",
                "Деревня",
                "п.",
                "Поселок",
                "кж",
            ]:
                addr.city = suffix
            elif prefix.strip() in [
                "ул.",
                "Улица",
                "алл.",
                "Аллея",
                "ш.",
                "Шоссе",
                "пр.",
                "Проспект",
                "пер.",
                "Переулок",
                "бул.",
                "Бульвар",
                "наб.",
            ]:
                addr.street = suffix

        else:
            part = part.strip().replace("э", "3").replace("о", "0").replace("s", "5")
            if len("".join([c for c in part if c.isdigit()])) >= 4:
                addr.index = "".join([c for c in part if c.isdigit()])

    return ",".join([addr.city, addr.street, addr.house, addr.flat, addr.index])
