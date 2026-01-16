using System;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using CsvHelper;
using Humanizer;
using Keras;
using Keras.Models;
using Numpy;
using Python.Runtime;

namespace CNNAlgorithm
{
    class Program
    {
		static void Main(string[] args)
		{

			var strPathCSV = GetFullPath(@"Dataset\sudoku.csv.gz");
			var strPathModel = GetFullPath(@"Models\sudoku.model");
			var nbSudokus = 1000;

			var stopW = Stopwatch.StartNew();


			var sudokus = DataSetHelper.ParseCSV(strPathCSV, nbSudokus);

			var testSudoku = sudokus[0];
			Console.Write($"Sudoku to solve:\n{testSudoku.Quiz.ToString()}");
			Console.Write($"Given Solution :\n{testSudoku.Solution.ToString()}");
			var preTrainedModel = NeuralNetHelper.LoadModel(strPathModel);

			var solvedWithNeuralNet = NeuralNetHelper.SolveSudoku(testSudoku.Quiz, preTrainedModel);
			Console.Write($"Solved with Neural Net :\n{solvedWithNeuralNet.ToString()}");


			Console.WriteLine($"Time Elpased: {stopW.Elapsed.Humanize(5)}");
			Console.ReadLine();
		}

		static string GetFullPath(string relativePath)
		{
			return System.IO.Path.Combine(Environment.CurrentDirectory, @"..\..\..\" + relativePath);
		}
	}
}