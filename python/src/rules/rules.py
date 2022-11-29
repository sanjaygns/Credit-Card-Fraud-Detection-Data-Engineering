def rules_check(ucl, score, speed, amount) -> bool:
    return False if score <200 or amount > ucl or speed >250 else True