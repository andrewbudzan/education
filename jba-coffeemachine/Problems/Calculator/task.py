# put your python code here

val1 = float(input())
val2 = float(input())
to_do = input()


def calculate(x=val1, y=val2, operation=to_do):
    if operation in ['/', 'mod', 'div'] and y == 0:
        print('Division by 0!')
    else:
        if operation == '+':
            print(x + y)
        elif operation == '-':
            print(x - y)
        elif operation == '/':
            print(x / y)
        elif operation == '*':
            print(x * y)
        elif operation == 'mod':
            print(x % y)
        elif operation == 'pow':
            print(x ** y)
        elif operation == 'div':
            print(x // y)


calculate()
