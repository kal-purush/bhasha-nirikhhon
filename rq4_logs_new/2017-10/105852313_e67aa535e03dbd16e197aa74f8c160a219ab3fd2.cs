using System;

public class Test
{
	public static void Main()
	{
		int t = int.Parse(Console.ReadLine());
		while(t-->0)
		{
			int a = int.Parse(Console.ReadLine());
			if(a % 2 == 0)
			Console.WriteLine("Thankyou Shaktiman");
			else
			Console.WriteLine("Sorry Shaktiman");
		}
	}
}