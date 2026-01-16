using BepInEx;
using BepInEx.Configuration;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace SupplementaryMines.config
{
    internal static class Config
    {
        private const string CONFIG_FILE_NAME = "supplementary_mines.cfg";

        private static ConfigFile _config;
        private static ConfigEntry<bool> _consistentLandmines;
        private static ConfigEntry<float> _landminesMultiplier;

        private static void Init()
        {
            Plugin.Log("Initializing config...");
            var filePath = Path.Combine(Paths.ConfigPath, CONFIG_FILE_NAME);
            _config = new ConfigFile(filePath, true);
            _consistentLandmines = _config.Bind("Config", "Consistent Landmines", false, "Every map will have mines in their spawn table.");
            _landminesMultiplier = _config.Bind("Config", "Landmines Multiplier", 1.0f, "Multiplies the default amount of mines to spawn.");
            Plugin.Log("Config initialized!");
        }

        public static bool consistentLandmines => _consistentLandmines.Value;
        public static float landminesMultiplier => _landminesMultiplier.Value;
    }
}