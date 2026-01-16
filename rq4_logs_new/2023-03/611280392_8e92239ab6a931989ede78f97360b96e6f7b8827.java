package Interviews;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Date : 18 Mar, 23
 * Find largest sub array whose sum is equal to 0
 */
public class Ptm
{
	public static void main(String[] args) throws Exception
	{
		System.out.println(retrieveLargestSubArrayWithSum0(new int[] {0, 2, -2, -4, 4, 5}));
		System.out.println(retrieveLargestSubArrayWithSum0(new int[] {0, 2, -2, 4, 4, 5}));
		System.out.println(retrieveLargestSubArrayWithSum0(new int[] {6, 2, -2, 4, 4, 5}));
	}

	public static List<Integer> retrieveLargestSubArrayWithSum0(int[] inputArray)
	{
		List<Integer> subArrayWithLargestSum = new ArrayList<>();
		int left = 0, right = inputArray.length - 1;
		while(left < right)
		{
			int sum = 0;
			for(int i = left; i <= right; i++)
			{
				sum += inputArray[i];
			}
			if(sum == 0)
			{
				subArrayWithLargestSum = Arrays.stream(inputArray)
					.boxed()
					.collect(Collectors.toList())
					.subList(left, right + 1);
				return subArrayWithLargestSum;
			}
			if(inputArray[left] > inputArray[right])
			{
				if(sum - inputArray[left] >= 0)
					left++;
				else
					right--;
			}
			else
			{
				if(sum - inputArray[right] >= 0)
					right--;
				else
					left++;
			}
		}
		return subArrayWithLargestSum;
	}
}