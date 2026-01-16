def rearrange_array(nums, n):
  """
  Rearranges the elements in the array nums in the form [x1,y1,x2,y2,...,xn,yn].

  Args:
    nums: The array to be rearranged.
    n: The number of elements in the array.

  Returns:
    The rearranged array.
  """

  rearranged_nums = []
  for i in range(n):
    rearranged_nums.append(nums[i])
    rearranged_nums.append(nums[n + i])
  return rearranged_nums


if __name__ == "__main__":
  nums = [2, 5, 1, 3, 4, 7]
  n = 3
  print(rearrange_array(nums, n))