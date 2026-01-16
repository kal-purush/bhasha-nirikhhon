using AnagramSolver.EF.DatabaseFirst.Models;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;

namespace AnagramSolver.BusinessLogic.Classes
{
    public class PersistentRepositoryWithEF
    {
        public void PopulateDataBaseFromFile()
        {
            var filePath = ReadSetting("FilePath");
            var minLength = int.Parse(ReadSetting("MinWordLength"));

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
            using (var context = new VocabularyDBContext())
            {
                foreach (var word in vocabulary)
                {
                    var wordToAdd = new Word { Word1 = word };
                    context.Words.Add(wordToAdd);
                    context.SaveChanges();
                }
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