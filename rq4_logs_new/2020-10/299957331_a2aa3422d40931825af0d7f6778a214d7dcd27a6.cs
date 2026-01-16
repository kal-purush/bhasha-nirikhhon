using System;
using System.IO;
using System.Reflection;

namespace TravellingSalespersonProj
{
    public class FileReader
    {
        public void ReadFileOfTypeCSV(Graph graph)
        {
            /*
            var assembly = Assembly.GetExecutingAssembly();
            var resourceName = @"..\Resources\ulysses.resx";

            using (Stream stream = assembly.GetManifestResourceStream(resourceName))
            {
                using (StreamReader reader = new StreamReader(stream))
                {
                    string result = reader.ReadToEnd();
                }
            }
            */

            try
            {
                // Open the text file using a stream reader.
                using (var sr = new StreamReader("C:\\Users\\joshu\\Documents\\repos\\TravelingSalesperson\\TravellingSalespersonProj\\TravellingSalespersonProj\\Resources\\ulysses.csv"))
                {
                    while (!sr.EndOfStream)
                    {
                        string[] values = sr.ReadLine().Split(',');

                        if(IsRowWithCorrectStartingData(values))
                        {
                            float[] coordinates = { float.Parse(values[1]), float.Parse(values[2]) };
                            graph.AddNodeToGraph(int.Parse(values[0]), coordinates);

                            Console.WriteLine($"{values[0]} {values[1]} {values[2]}");
                        }
                    }
                }
            }
            catch (IOException e)
            {
                Console.WriteLine("The file could not be read:");
                Console.WriteLine(e.Message);
            }

        }

        private bool IsRowWithCorrectStartingData(string[] row)
        {
            int result = 0;
            if (int.TryParse(row[0], out result) && row[0] != string.Empty)
            {
                return true;
            }

            return false;
        }

        /*
        public void ReadFileOfTypeCSVTwo(Graph graph)
        {
            // path to the csv file
            string path = "C:\\Users\\joshu\\Documents\\repos\\TravelingSalesperson\\TravellingSalespersonProj\\TravellingSalespersonProj\\Resources\\ulysses.csv";

            string[] lines = System.IO.File.ReadAllLines(path);
            foreach (string line in lines)
            {
                string[] columns = line.Split(',');
                foreach (string column in columns)
                {
                    float[] coordinates = { float.Parse(column[1]), float.Parse(column[2]) };
                    graph.AddNodeToGraph(int.Parse(column[0]), coordinates);
                }
            }
        }
        */
    }
}