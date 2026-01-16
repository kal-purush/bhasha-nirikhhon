using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LeetCode.ArrayString
{
	public class DiagonalTraverse
	{
		//498. Diagonal Traverse
		//Given an m x n matrix mat, return an array of all the elements of the array in a diagonal order.

		//Example 1:
		//Input: mat = [[1,2,3],[4,5,6],[7,8,9]]
		//Output: [1,2,4,7,5,3,6,8,9]
		public int[] FindDiagonalOrder(int[][] mat)
		{

			//There are m+nm+nm+n diagonals to traverse (where mmm is the number of rows and nnn is the number of columns).
			//Assuming ddd is the index of the diagonal we are currently traversing:
			//when going up (up==trueup==trueup==true)
			//r index starts at min between ddd and m−1m-1m−1
			//c index starts at d−rd-rd−r (since r+cr+cr+c is equal to ddd)
			//every iteration we decrease rrr and increase ccc, as far as ccc is inside the allowed bound.

			//when going down (up==falseup==falseup==false
			//c index starts at min between d and n−1
			//r index starts at d−c (since r+c is equal to d)
			//every iteration we decrease ccc and increase rrr, as far as rrr is inside the allowed bound.

			//Runtime: 143 ms
			//Memory: 54.58 MB
			int m = mat.Length;
			int n = mat[0].Length;
			int[] result = new int[m * n];
			int i = 0;
			int nDiag = m + n;
			bool up = true;
			for (int d = 0; d <= nDiag; d++, up = !up)
			{
				int r = up ? Math.Min(d, m - 1) : d - Math.Min(d, n - 1);
				int c = up ? d - Math.Min(d, m - 1) : Math.Min(d, n - 1);

				while (up && r >= 0 && c < n) result[i++] = mat[r--][c++];

				while (!up && c >= 0 && r < m) result[i++] = mat[r++][c--];
			}
			return result;
		}
	}
}