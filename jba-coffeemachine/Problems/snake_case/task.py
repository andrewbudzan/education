sample = list(input())
output = []
for letter in sample:
    if not letter.isupper():
        output.append(letter)
    else:
        output.append('_' + letter.lower())
print(''.join(output))
