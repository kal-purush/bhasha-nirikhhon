'''
The following problem givesgood insight into working with arrays: Your input is an
array of integers, and you have to reorder its entriesso that the even entries appear
first. Thisis easy if you use 0(n)space, where n is the length of the array. However,
you are required to solve it without allocating additionalstorage.

When working with arrays you should take advantage of the fact that you can
operate efficiently on both ends.

Complexity:

    Time complexity: O(n) //goes through every array element
    Space complexity O(1) //in-place re-order of array
'''

# Algorithm works with two 'indices' for both ends, and shifts them (and their values) in the array in-place as necessary.
def even_elements_first(nums):

    # consider edge cases like empty or None collection here, etc...

    even_index, odd_index = 0, len(nums) - 1
    
    while even_index < odd_index:
        if nums[even_index] % 2 == 0:
            even_index += 1
        else:
            aux = nums[even_index]
            nums[even_index] = nums[odd_index]
            nums[odd_index] = aux
            odd_index -= 1

if __name__ == "__main__":
    nums = [1,2,3,4,5,6,7,8,9,10]
    even_elements_first(nums)
    print(f"Array re-ordered with even elements first: {nums}")