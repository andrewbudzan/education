

def even_numbers(input_value):
    x = 1
    while x < input_value:
        if x % 2 == 0:
            print(x)
        else:
            pass
        x += 1


val = int(input())
even_numbers(val)

