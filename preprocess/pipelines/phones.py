def parse_symbols(phone: str) -> str:
    return phone.replace("э", "3").replace("s", "5").replace("i", "1")


def remove_other_symbols(phone: str) -> str:
    phone = phone.replace("+7", "8")
    return "".join([c for c in phone if c.isdigit()])
