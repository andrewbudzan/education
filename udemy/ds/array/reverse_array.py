# author: andrii budzan

def reversed(nums):
    start_index = 0
    end_index = len(nums) - 1

    while end_index > start_index:
        nums[start_index], nums[end_index] = nums[end_index], nums[start_index]
        start_index += 1
        end_index -= 1

    return nums


if __name__ == '__main__':
    nums = [1, 2, 3, 4, 5]
    rnums = reversed(nums.copy())
    print(nums)
    print(rnums)
