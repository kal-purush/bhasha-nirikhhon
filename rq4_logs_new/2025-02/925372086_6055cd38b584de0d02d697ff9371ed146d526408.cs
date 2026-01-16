
using System;

namespace at09
{
	class Program
	{
		public static void Main(string[] args)
		{
			Console.WriteLine("Digite dois números inteiros");
			Console.Write("n°1: ");
			int num1 = int.Parse(Console.ReadLine());
			
			Console.Write("n°2: ");
			int num2 = int.Parse(Console.ReadLine());
			
			int diferenca = Math.Abs(num1 - num2);
			
			if (diferenca <= 10) {
				Console.WriteLine("A diferença é menor ou igual a 10");
			} else {
				Console.WriteLine("False");
			}
			
			
			Console.Write("Press any key to continue . . . ");
			Console.ReadKey(true);
		}
	}
}