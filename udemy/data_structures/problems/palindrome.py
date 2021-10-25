# author: andrii budzan

# simplest implementation using python language features
def is_palindrome_pythonic(s):
    if s == s[::-1]:
        return True
    return False


# more general implementation:
# in this case O(N) linear running time where N is the number of letters in string s N=len(s)
def reversed(data):

    # string to list of characters
    data = list(data)
    start_index = 0
    end_index = len(data) - 1

    while end_index > start_index:
        data[start_index], data[end_index] = data[end_index], data[start_index]
        start_index += 1
        end_index -= 1

    # transform list of letters into string
    return ''.join(data)

# it has 0(s) so basically linear running time complexity
# as far as the number of the letters in the string is concerned
def is_palindrome_general(s):
    original_string = s
    # function from previous task in O(N)
    reversed_string = reversed(s)
    if original_string == reversed_string:
        return True
    return False




if __name__ == '__main__':
    s = 'madam'
    print('pythonic')
    print(is_palindrome_pythonic(s))
    print('general')
    print(is_palindrome_general(s))
