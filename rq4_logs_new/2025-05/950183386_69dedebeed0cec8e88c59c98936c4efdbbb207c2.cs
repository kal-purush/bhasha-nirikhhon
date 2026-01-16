using System.Collections.Generic;
using WelwiseGamesSDK.Shared;

namespace WelwiseGamesSDK.Internal.PlayerData
{
    internal sealed class DataContainer : IData
    {
        public bool Changed;
        public readonly Dictionary<string, float> Floats = new();
        public readonly Dictionary<string, string> Strings = new();
        public readonly Dictionary<string, int> Ints = new();
        public readonly Dictionary<string, bool> Booleans = new();

        public void Clear()
        {
            Floats.Clear();
            Strings.Clear();
            Ints.Clear();
            Booleans.Clear();
        }

        public void SetString(string key, string value)
        {
            Strings[key] = value;
            Changed = true;
        }

        public void SetInt(string key, int value)
        {
            Ints[key] = value;
            Changed = true;
        }

        public void SetFloat(string key, float value)
        {
            Floats[key] = value;
            Changed = true;
        }

        public void SetBool(string key, bool value) 
        {
            Booleans[key] = value;
            Changed = true;
        }
        
        public string GetString(string key, string defaultValue) => Strings.GetValueOrDefault(key, defaultValue);
        public int GetInt(string key, int defaultValue) => Ints.GetValueOrDefault(key, defaultValue);
        public float GetFloat(string key, float defaultValue) => Floats.GetValueOrDefault(key, defaultValue);
        public bool GetBool(string key, bool defaultValue) => Booleans.GetValueOrDefault(key, defaultValue);
    }
}