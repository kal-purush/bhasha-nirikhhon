using Newtonsoft.Json;
using Plugin.Settings;
using System;
using System.Collections.Generic;
using System.Text;

namespace iotc_xamarin_ble.Services.Storage
{
    public class PreferencesStorage : IStorage
    {
        private static PreferencesStorage storage;
        private readonly Dictionary<string, string> prefStorage;

        private const string USER_PREFERENCES = "user_preferences";

        public static PreferencesStorage Current
        {
            get
            {
                if (storage == null)
                    storage = new PreferencesStorage();
                return storage;
            }
        }

        private PreferencesStorage()
        {
            if (CrossSettings.Current.Contains(USER_PREFERENCES))
            {
                prefStorage = JsonConvert.DeserializeObject<Dictionary<string, string>>(CrossSettings.Current.GetValueOrDefault(USER_PREFERENCES, "{}"));
            }
            else
            {
                prefStorage = new Dictionary<string, string>();
            }
        }

        public void Add(string key, string value)
        {
            if (!prefStorage.ContainsKey(key))
            {
                prefStorage.Add(key, value);
            }
            else prefStorage[key] = value;
        }

        public void Save()
        {
            CrossSettings.Current.AddOrUpdateValue(USER_PREFERENCES, JsonConvert.SerializeObject(prefStorage));
        }

        public string this[string key]
        {
            get
            {
                if (prefStorage.ContainsKey(key))
                {
                    return prefStorage[key];
                }
                return null;
            }
        }

        public Dictionary<string, string> GetAll()
        {
            return prefStorage;
        }

        public void Remove(string key)
        {
            if (!prefStorage.ContainsKey(key))
            {
                return;
            }
            else prefStorage.Remove(key);
        }
    }
}