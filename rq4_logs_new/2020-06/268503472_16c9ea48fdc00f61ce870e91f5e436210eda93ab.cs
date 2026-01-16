using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Channels;
using System.Xml.Serialization;
using Newtonsoft.Json;

namespace ListShuffling
{
    class Program
    {
        static void Main(string[] args)
        {
            var configFile = JsonConvert.DeserializeObject<Config>(File.ReadAllText("appsettings.json"));
            var shuffler = new Shuffler(configFile);
            Console.WriteLine($"Creating FileHandler for type {configFile.FileNames.FileType} using FileHandlerFactory");
            var shuffledList = shuffler.GetShuffledListFromFile();
            Console.WriteLine($"Retrieved and shuffled lists from {configFile.FileNames.InputFileName}");
            if (configFile.ShuffleType.OneByOneShuffle)
            {
                Console.WriteLine($"Shuffling using {nameof(configFile.ShuffleType.OneByOneShuffle)}");
            }
            else
            {
                Console.WriteLine($"Shuffling using {nameof(configFile.ShuffleType.RandomShuffle)}");
            }
            shuffler.SaveShuffledListToFile(shuffledList);
            Console.WriteLine($"Saved shuffled list to {configFile.FileNames.OutputFileName}");
        }
    }
}