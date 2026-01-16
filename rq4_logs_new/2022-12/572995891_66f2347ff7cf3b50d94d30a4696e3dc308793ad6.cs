namespace Day9
{
    public static class Part1
    {
		public static void Run()
		{
			var solutionDirectory = Directory.GetParent(Environment.CurrentDirectory)?.Parent?.Parent?.Parent?.FullName;
			var inputFilepath = $"{solutionDirectory}\\InputFiles\\Day9 test.txt";
			var reader = new StreamReader(inputFilepath);
			var nextLine = string.Empty;

			var hRow = 0;
			var hCol = 0;
			var tRow = 0;
			var tCol = 0;
			var visited = new List<(int, int)>
            {
				(tRow, tCol)
            };

			while ((nextLine = reader.ReadLine()) != null)
			{
				var parts = nextLine.Split(' ');
				var direction = parts[0];
				var distance = int.Parse(parts[1]);

				// Up = negative row, Down = positive row
				if (direction == "U" || direction == "D")
                {
					var dir = 1;
					if (direction == "U") dir = -1;

                    for (int i = 1; i <= distance; i++)
                    {
						// Move the head
						var hRowPrevious = hRow;
						hRow += (i * dir);

						if (MathF.Abs(hRow - tRow) > 1)
                        {
							// Move the tail
							tRow = hRowPrevious;
							if (!visited.Contains((tRow, tCol))) visited.Add((tRow, tCol));
                        }
                    }

					continue;
                }

				// Left = negative col, Right = positive col
				if (direction == "L" || direction == "R")
				{
					var dir = 1;
					if (direction == "L") dir = -1;

					for (int i = 1; i <= distance; i++)
					{
						// Move the head
						var hColPrevious = hCol;
						hCol += (i * dir);

						if (MathF.Abs(hCol - tCol) > 1)
						{
							// Move the tail
							tCol = hColPrevious;
							if (!visited.Contains((tRow, tCol))) visited.Add((tRow, tCol));
						}
					}

					continue;
				}

				Console.WriteLine("Invalid direction");
			}

            foreach (var item in visited)
            {
				Console.WriteLine(item);
            }

			Console.WriteLine($"Tail visited {visited.Count()} locations.");
		}
    }
}