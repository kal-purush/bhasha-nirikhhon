using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSGO_Dedicated_Server.Utilities
{
    class Settings
    {
        public string Path { get; set; }
        public int GameMode { get; set; }

        public static void SaveToFile(Settings settings)
        {
            // JSON save
        }

        public static Settings LoadFromFile(string path)
        {
            // Json load from file 
            return new Settings();
        }
    }
}