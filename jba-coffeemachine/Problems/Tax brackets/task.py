value = int(input())

def percent_check(income):
    if 0 <= income <= 15527:
        return 0
    elif 15528 <= income <= 42707:
        return 15
    elif 42708 <= income <= 132406:
        return 25
    elif income >= 132407:
        return 28


tax_percent = percent_check(value)
message = f'The tax for {value} is {tax_percent}%. That is {round((value * tax_percent / 100))} dollars!'
print(message)
