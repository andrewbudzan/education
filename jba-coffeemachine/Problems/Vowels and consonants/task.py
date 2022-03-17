import string
sample = list(input())
alphabet = string.ascii_lowercase
vowels = '[aeiou]'
for letter in sample:
    if letter not in alphabet:
        break
    elif letter in vowels:
        print('vowel')
    else:
        print('consonant')
