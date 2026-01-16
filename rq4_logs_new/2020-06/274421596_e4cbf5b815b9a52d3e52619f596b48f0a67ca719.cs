//Write a program that reads a rectangular integer matrix of size N x M and finds in it the square 3 x 3 that has maximal sum of its elements.
//Input
//On the first line, you will receive the rows N and columns M.On the next N lines you will receive each row with its columns
//Output
//Print the elements of the 3 x 3 square as a matrix, along with their sum

using System;
using System.Linq;

namespace MaximalSum
{
	class Program
	{
		static void Main(string[] args)
		{
			int[] matrixSize = Console.ReadLine()
				.Split(' ', StringSplitOptions.RemoveEmptyEntries)
				.Select(int.Parse)
				.ToArray();
			int[,] matrix = new int[matrixSize[0], matrixSize[1]];

			for (int row = 0; row < matrix.GetLength(0); row++)
			{
				int[] colElements = Console.ReadLine()
					.Split(' ', StringSplitOptions.RemoveEmptyEntries)
					.Select(int.Parse)
					.ToArray();
				for (int col = 0; col < matrix.GetLength(1); col++)
				{
					matrix[row, col] = colElements[col];
				}
			}

			int sum = 0;
			int maxSum = int.MinValue;
			int targetRow = 0;
			int targetCol = 0;

			for (int row = 0; row < matrix.GetLength(0) - 2; row++)
			{
				for (int col = 0; col < matrix.GetLength(1) - 2; col++)
				{
					sum = matrix[row, col] + matrix[row, col + 1] + matrix[row, col + 2]
						+ matrix[row + 1, col] + matrix[row + 1, col + 1] + matrix[row + 1, col + 2]
						+ matrix[row + 2, col] + matrix[row + 2, col + 1] + matrix[row + 2, col + 2];

					if (sum > maxSum)
					{
						maxSum = sum;
						targetRow = row;
						targetCol = col;
					}
				}
			}

			Console.WriteLine($"Sum = {maxSum}");

			for (int row = targetRow; row <= targetRow + 2; row++)
			{
				for (int col = targetCol; col <= targetCol + 2; col++)
				{
					Console.Write($"{matrix[row, col]} ");
				}
				Console.WriteLine();
			}

		}
	}
}