using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace day4_Q
{
	class Program
	{
		static void Main(string[] args)
		{
			// 1
			for(int i = 0; i <= 3; i++)
			{
				for(int k = 3; k >= i; k--)
				{
					Console.Write(" ");
				}

				for (int j = 0; j <= i * 2 ; j++)
				{
					Console.Write("*");
				}
				Console.WriteLine();
			}

			Console.WriteLine();
			Console.ReadKey();


			//2
			int a = 3;
			
			while (a >= 0)// 3210
			{
				int c = 0;
				while (c>3) // 0
				{
					Console.Write("@");
					c++;
				}

				int b = 2 * a;
				while (b >= 0)// 7 5 3 1
				{
					Console.Write("*");
					b--;
				}
				Console.WriteLine();
				a--;
			}




			// 3
			while (true)
			{
				Console.Write("몇 번 반복할까요?\n");
				int num = int.Parse(Console.ReadLine());

				for(int i = 0; i<num; i++)
				{
					for(int k = num; k>=i; k--)
					{
						Console.Write(" ");
					}

					for(int j = 0; j<= i*2 ;j++)
					{
						Console.Write("*");
					}
					Console.WriteLine();
				}
				if (num == 0)
					Console.WriteLine("0으로는 반복문을 수행할 수 없어요.");
				continue;
			}





		}
	}
}