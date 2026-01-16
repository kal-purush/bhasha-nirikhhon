using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Jelado_proj
{
    class JsonParser
    {
        public static List<T> Read<T>(string allomanyNeve)
        {
            string json_data = File.ReadAllText(allomanyNeve, encoding: UTF8Encoding.UTF8);
            return JsonConvert.DeserializeObject<List<T>>(json_data);
        }

        public static void Write<T>(string allomanyNeve, List<T> adatok)
        {
            string json = JsonConvert.SerializeObject(adatok);
            File.WriteAllText(allomanyNeve, json);
        }
    }
}