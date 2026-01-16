package permutation;

public class Permutator
{
	private int[] list;
	private int[] fact;
	private int iteration;

	public Permutator(int numElements) 
	{
		iteration = 1;
		list = new int[numElements];
		fact = new int[numElements + 1];
		for(int i = 0; i < list.length; i++) 
		{
			list[i] = i;
			fact[i] = factorial(i);
		}
		fact[numElements] = factorial(numElements);
	}
	
	public int[] getPermuationReference() 
	{
		return list;
	}
	
	public int[] getPermutation() 
	{
		int[] result = new int[list.length];
		for(int i = 0; i < list.length; i++)
			result[i] = list[i];
		return result;
	}
	
	public void permutate()
	{
		int step = 1;
		while (iteration % fact[step] == 0)
			step++;
		step--;
		if (step == 1)
			swap(list.length - 1, list.length - 2);
		else
		{
			int toSwapWith = minIndexGreaterThan(list[list.length - 1 - step], list.length - step);
			swap(list.length - step - 1, toSwapWith);
			reverse(list.length - step);
		}
		iteration++;
	}

	private void reverse(int start)
	{
		for (int i = 0; i < (list.length - 1 - start) / 2 + 1; i++)
		{
			int temp = list[start + i];
			list[start + i] = list[list.length - 1 - i];
			list[list.length - 1 - i] = temp;
		}
	}

	private void swap(int a, int b)
	{
		int temp = list[a];
		list[a] = list[b];
		list[b] = temp;
	}

	private int minIndexGreaterThan(int greaterThan, int start)
	{
		Integer min = null;
		for (int i = start; i < list.length; i++)
		{
			if (list[i] > greaterThan)
			{
				if (min == null)
					min = i;
				else if (list[i] < list[min])
					min = i;
			}
		}
		return min;
	}

	public int factorial(int a)
	{
		int product = 1;
		for (int i = 2; i <= a; i++)
			product *= i;
		return product;
	}
}