# put your python code here
sample = []
zero_sum = False
while not zero_sum:
    sample.append(int(input()))
    if sum(sample) != 0:
        # print(f'sample: {sample}')
        # print(f'sum: {sum(sample)}')
        # print(f'squares: {[x * x for x in sample]}')
        # print(f'sq_sum: {sum([x * x for x in sample])}')
        continue
    else:
        # print(f'sample: {sample}')
        # print(f'sum: {sum(sample)}')
        # print(f'squares: {[x * x for x in sample]}')
        # print(f'sq_sum: {sum([x * x for x in sample])}')
        # print('finish')
        zero_sum = True
        print(sum([x * x for x in sample]))
