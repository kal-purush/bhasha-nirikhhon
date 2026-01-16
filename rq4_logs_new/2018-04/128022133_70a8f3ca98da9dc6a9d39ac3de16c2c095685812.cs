using System;
using System.Collections.Generic;
using System.IO;
using System.Windows.Forms;

namespace task0
{
	class Program
	{
		[STAThread]
		static void Main(string[] args)
		{
			Console.OutputEncoding = System.Text.Encoding.Default;
			//Console.BackgroundColor = ConsoleColor.DarkYellow;
			//Console.ForegroundColor = ConsoleColor.Magenta;

			const double cashPercent = 0.4;
			string[] strings;
			List<Student> students = new List<Student>();

			OpenFileDialog ofd = new OpenFileDialog();
			if (ofd.ShowDialog() == DialogResult.OK)
			{
				System.Diagnostics.Stopwatch t = System.Diagnostics.Stopwatch.StartNew();
				string path = Path.GetDirectoryName(ofd.FileName);
				strings = File.ReadAllLines(ofd.FileName);
				int count = Int32.Parse(strings[0]);
				for (int s = 1; s <= count; s++)
				{
					students.Add(new Student(strings[s]));
				}

				List<Student> budg = new List<Student>(); //budget and honest guys
				students.ForEach((s) =>
				{
					if (!s.IsContract)
						budg.Add(s);
				});

				budg.Sort((s1, s2) => Math.Sign(s2.Average - s1.Average));

				Console.WriteLine("Стипендіати:");
				int i = 0;
				for (; i < Math.Floor(budg.Count * cashPercent); i++)
				{
					Student s = budg[i];
					Console.WriteLine($"{i + 1}.\t{s.Name.PadRight(20)}{s.Average.ToString("0.000")}");
				}
				Console.WriteLine("\nПрохідний бал: " + budg[i - 1].Average.ToString("0.000"));
				Console.WriteLine("\nExecution time:\t" + t.ElapsedMilliseconds + "ms.");
				Console.WriteLine("Memory: " + System.Diagnostics.Process.GetCurrentProcess().PrivateMemorySize64.ToString("#,0"));
			}
			Console.ReadKey();
		}
	}
	class Student
	{
		public string Name { get; set; }
		public int[] Points { get; set; } = new int[5];
		public bool IsContract { get; set; }
		public double Average { get; set; }
		public Student(string toParse)
		{
			string[] vals = toParse.Split(',');
			Name = vals[0];
			for (int i = 0; i < 5; i++)
			{
				Points[i] = Int32.Parse(vals[i + 1]);
			}
			IsContract = (vals[6][0] == 'T');
			Average = GetAverage(Points);
		}
		private static double GetAverage(int[] pnts)
		{
			double res = 0;
			foreach (int i in pnts)
				res += i;
			return Math.Round(res / pnts.Length, 3);
		}
	}
}