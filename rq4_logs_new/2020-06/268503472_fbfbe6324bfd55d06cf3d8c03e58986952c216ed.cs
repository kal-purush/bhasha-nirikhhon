using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;

namespace ListShuffling
{
    public class CsvFileHandler : IFileHandler
    {
        public (List<string>, List<string>) ReadListsFromFileToTupleOfArrays(string filePath)
        {
            try
            {
                var lines = File.ReadAllLines(filePath);
                if (lines.Length >= 2)
                {
                    (List<string>, List<string>) inputTuple;
                    inputTuple.Item1 = lines[0].Split(", -\t; ".ToCharArray()).ToList();
                    inputTuple.Item2 = lines[1].Split(", -\t; ".ToCharArray()).ToList();
                    return inputTuple;
                }
                else
                {
                    throw new ArgumentException("CSV file has not enough lists!");
                }

            }
            catch (FileNotFoundException)
            {
                Console.WriteLine($"File not found in path {filePath}!");
                throw new ArgumentException("Invalid file path!");
            }
            catch (ArgumentException e)
            {
                Console.WriteLine(e.Message);
                throw;
            }
        }

        public void WriteShuffledListsToFile(string filePath, List<string> shuffledList)
        {
            File.WriteAllText(filePath, string.Join(",", shuffledList));
        }
    }
}