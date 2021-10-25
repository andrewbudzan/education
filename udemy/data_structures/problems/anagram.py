# author: andrii budzan

def is_anagram(str1, str2):

    if len(str1) != len(str2):
        return False

    # sort letters and compare letters with same indexes
    # bottleneck because it has O(NlogN)
    str1 = sorted(str1)
    str2 = sorted(str2)

    for i in range(len(str1)):
        if str1[i] != str2[i]:
            return False
    return True


if __name__ == '__main__':
    s1 = list('fluster')
    s2 = list('restful')
    print(is_anagram(s1, s2))
