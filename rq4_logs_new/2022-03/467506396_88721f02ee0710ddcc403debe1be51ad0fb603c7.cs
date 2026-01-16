using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

public class Problem
{
	public int numberOfElements;
	public int seed;
	public int capacity;
	public List<Item> listOfItems;

	public Problem()
	{
		numberOfElements = 0;
		seed = 0;
		capacity = 0;
		listOfItems = new List<Item>();
	}

	public Problem(int nof, int s, int c)
    {
		numberOfElements = nof;
		seed = s;
		capacity = c;
		listOfItems = new List<Item>();
	}

	public void Elements() 
    {
		Random random = new Random(seed);
		int []wv = new int[2];

		for (int i = 0; i < numberOfElements; i++)
        {
			wv[0] = random.Next(capacity-1);
			wv[1] = random.Next(capacity-1);
			listOfItems.Add(new Item(wv[0], wv[1], i));
        }
    }

	public void Solution() //typ zwracany - result
    {
		listOfItems.Sort()
    }
}