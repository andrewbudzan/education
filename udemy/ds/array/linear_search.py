# author: andrii budzan

array = [10, 44, 66, 12, 2, 77, 43]

# random indexing: indexes starts from 0

# linear search: O(N)

max = array[0]
for num in array:
    if num > max:
        max = num
print(max)


min = array[0]
for num in array:
    if num < min:
        min = num
print(min)
