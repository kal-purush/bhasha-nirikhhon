using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Channels;
using Newtonsoft.Json;

namespace FileAlphanumericCounter
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length == 1 && (args[0].Equals("-h") || args[0].Equals("--help")))
            {
                Console.WriteLine("Pass path to file");
                Console.WriteLine("Example: FileAlphanumericCounter file.txt");
            }
            else if (args.Length == 0)
            {
                Console.WriteLine("You need to pass an argument!");
                Console.WriteLine("Correct usage: FileAlphanumericCounter <path_to_file>");
            }
            else if (File.Exists(args[0]))
            {
                var configFile = JsonConvert.DeserializeObject<FilesConfig>(File.ReadAllText("appsettings.json"));
                if (configFile.AllowedExtensions.Contains(Path.GetExtension(args[0])))
                {
                    var fileHandler = new FileHandler(configFile);
                    var filteredText = fileHandler.ReadFilteredTextFromFile(args[0]);
                    var charactersCount = filteredText.GetCharactersCount();
                    var newFilePath = $"{Path.GetFileNameWithoutExtension(args[0])}_stats.txt";
                    fileHandler.WriteToFile(newFilePath, charactersCount.FormHighestAndLowestFrequencyContent());
                }
                else
                {
                    Console.WriteLine($"ERROR: Invalid extension! Allowed extensions are: [{string.Join(",", configFile.AllowedExtensions)}]!");
                }
            }
            else
            {
                Console.WriteLine("ERROR: Invalid path! File doesn't exist!");
            }
        }
    }
}