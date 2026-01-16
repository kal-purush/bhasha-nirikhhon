void Main()
{
	GetFibo().Take(10).Dump();
}

// Define other methods and classes here

public IEnumerable<int> GetFibo() 
{
	yield return 0;
	yield return 1;
	int i1 = 0;
	int i2 = 1;
	while (true) 
	{
		int fibo = i1 + i2;
 		i1 = i2;
		i2 = fibo;
		yield return fibo;	
	}
}