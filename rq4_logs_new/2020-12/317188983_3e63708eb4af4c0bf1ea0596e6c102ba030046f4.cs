using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Darkec.Models;
using Newtonsoft.Json;

namespace Darkec.Helpers
{
    public class JsonFileWriter<Key, Value>
    {
        public static void WriteToJson(Dictionary<Key, Value> apartments, string JsonFileName)
        {

            string output = JsonConvert.SerializeObject(apartments, Formatting.Indented);

            File.WriteAllText(JsonFileName, output);
        }
    }
}