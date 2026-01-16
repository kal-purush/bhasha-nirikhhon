using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LeetCode.ArrayString
{
	public class RotateMatrix
	{
		public static bool RotateMatrix90Degree(int[,] matriz)
		{
			int n = matriz.GetLength(0);
			if (n == 0 || n != matriz.GetLength(1)) return false;
			for (int i = 0; i < n; i++)
			{
				for (int j = 0; j < n; j++)
				{
					int temp = matriz[i,j];
					matriz[i, j] = matriz[j,i];
					matriz[j, i] = temp;
				}
			}
			for(int i = 0; i< n; i++)
			{
				for(int j =0; j< n /2; j++)
				{
					int temp = matriz[i, j];
					matriz[i, j] = matriz[i, n - 1 - j];
					matriz[i, n - 1 - j] = temp;
				}
			}

			return true;
		}
	}
}