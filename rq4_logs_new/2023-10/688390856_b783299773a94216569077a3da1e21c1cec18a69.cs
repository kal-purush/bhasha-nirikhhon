using System.IO;
using Newtonsoft.Json;
using UnityEngine;
using Application = UnityEngine.Device.Application;

namespace Spooky
{
    [JsonObject(MemberSerialization.OptIn)]
    public class GameConfig
    {
        public static string ConfigFilePath => Application.dataPath + "/config.json";

        private static GameConfig _config;

        public static GameConfig Config
        {
            get
            {
                if (_config != null)
                    return _config;

                if (!File.Exists(ConfigFilePath))
                {
                    _config = new GameConfig();
                    File.WriteAllText(ConfigFilePath, JsonConvert.SerializeObject(_config, Formatting.Indented));
                    Debug.Log("Config file created\n" + _config);
                    return _config;
                }

                string json = File.ReadAllText(ConfigFilePath);
                _config = JsonConvert.DeserializeObject<GameConfig>(json);
                Debug.Log("Config file loaded");
                
                File.WriteAllText(ConfigFilePath, JsonConvert.SerializeObject(_config, Formatting.Indented));
                Debug.Log("Config file updated");
                
                return _config;
            }
        }
        
        public override string ToString()
        {
            return JsonConvert.SerializeObject(this);
        }

        [JsonProperty("use_random_simon_sequence")]
        public bool UseRandomSimonSequence = false;

        [JsonProperty("game_name")] 
        public string GameName = "Lost and Sound";
    }
}