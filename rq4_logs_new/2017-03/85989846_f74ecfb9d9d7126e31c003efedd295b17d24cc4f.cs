using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace CSGO_Dedicated_Server.Utilities
{
    static class SettingsHelper
    {
        public static Settings LoadFromFile()
        {
            if (File.Exists(@".\settings.txt"))
            {
                var text = System.IO.File.ReadAllText(@".\settings.txt");
                var settings = JsonConvert.DeserializeObject<Settings>(text);
                return settings;
            }
            return new Settings { GameMode = 0, Map = "de_dust2", Path = "" };
        }
    }
}