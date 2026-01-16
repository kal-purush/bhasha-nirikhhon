using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AnagramSolver.BusinessLogic.Classes
{
    public class PersistentRepository
    {
        public void PopulateDataBaseFromFile()
        {
            var filePath = ReadSetting("FilePath");
            var connectionString = ReadSetting("ConnectionString");
            var minLength = int.Parse(ReadSetting("MinWordLength"));


            int id = 1;
            HashSet<string> fileLines;
            fileLines = new HashSet<string>(File.ReadLines(filePath));
            var vocabulary = new HashSet<string>();

            foreach (string line in fileLines)
            {
                string[] wordsInLine = line.Split("\t").ToArray();
                if (wordsInLine[2].Length >= minLength)
                {
                    var word = wordsInLine[2];
                    vocabulary.Add(word);
                }
            }

            try
            {
                var connection = new SqlConnection(connectionString);
                connection.Open();
                foreach (var word in vocabulary)
                {
                    var cmd = new SqlCommand("INSERT INTO Words (ID, Word) VALUES (@id, @word)", connection);
                    cmd.Parameters.AddWithValue("@id", id);
                    cmd.Parameters.AddWithValue("@word", word);
                    cmd.ExecuteNonQuery();
                    id++;
                }

                connection.Close();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }
        public string ReadSetting(string key)
        {

            var appSettings = ConfigurationManager.AppSettings;
            string specificSetting = appSettings[key] ?? "Not Found";
            return specificSetting;

        }
    }
}