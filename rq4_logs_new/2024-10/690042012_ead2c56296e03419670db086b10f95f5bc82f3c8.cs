using System;

namespace F01 {
	public class Program {
		public static void Main(string[] args) {
			string bemenet;
			string[] darabok;

			int n, k;

			bemenet = Console.ReadLine();
			darabok = bemenet.Split(' ', StringSplitOptions.RemoveEmptyEntries);

			n = int.Parse(darabok[0]);
			k = int.Parse(darabok[1]);

			int[] ar = new int[n + 1];

			for(int i = 1; i <= n; ++i) {
				bemenet = Console.ReadLine();
				ar[i] = int.Parse(bemenet);
			}

			bool van;
			int ind;

			van = false;
			ind = 1;

			while(ind <= n && ar[ind] >= k) {
				++ind;
			}

			van = ind <= n;
			if(!van) {
				ind = -1;
			}

			Console.WriteLine(ind);
		}
	}
}