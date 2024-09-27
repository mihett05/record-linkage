def remove_other_symbols(email: str) -> str:
    return "".join([c for c in email if c.isalnum()])


def slice_by_digits(email: str) -> str:
    start_index = 0
    for i in range(len(email)):
        if email[i].isdigit():
            start_index = i
            break

    end_index = 0
    for i in range(len(email) - 1, -1, -1):
        if email[i].isdigit():
            end_index = i
            break
    digit_part = "".join([c for c in email[start_index : end_index + 1] if c.isdigit()])
    return email[:start_index] + digit_part
