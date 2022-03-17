class RightTriangle:
    def __init__(self, hyp, leg_1, leg_2):
        self.c = hyp
        self.a = leg_1
        self.b = leg_2
        # calculate the area here
        self.area = self.a * self.b / 2


# triangle from the input
input_c, input_a, input_b = [int(x) for x in input().split()]


# write your code here
def square(x):
    return x * x


if square(input_c) == square(input_a) + square(input_b):
    triangle = RightTriangle(input_c, input_a, input_b)
    print(triangle.area)
else:
    print('Not right')