using System.Collections;
using System.Collections.Generic;
using UnityEngine;

namespace JevLogin
{
    public sealed class PlayerPrefsData : IData<SaveData>
    {
        public SaveData Load(string path = null)
        {
            var result = new SaveData();

            var key = XmlNameAttributes.Name;
            if (PlayerPrefs.HasKey(key))
            {
                result.Name = PlayerPrefs.GetString(key);
            }

             key = XmlNameAttributes.PositionX;
            if (PlayerPrefs.HasKey(key))
            {
                result.Position.X = PlayerPrefs.GetFloat(key);
            }

             key = XmlNameAttributes.PositionY;
            if (PlayerPrefs.HasKey(key))
            {
                result.Position.Y = PlayerPrefs.GetFloat(key);
            }

             key = XmlNameAttributes.PositionZ;
            if (PlayerPrefs.HasKey(key))
            {
                result.Position.Z = PlayerPrefs.GetFloat(key);
            }

             key = XmlNameAttributes.IsEnable;
            if (PlayerPrefs.HasKey(key))
            {
                result.IsEnabled = PlayerPrefs.GetString(key).TryBool();
            }

            return result;
        }

        public void Save(SaveData data, string path = null)
        {
            PlayerPrefs.SetString(XmlNameAttributes.Name, data.Name);
            PlayerPrefs.SetFloat(XmlNameAttributes.PositionX, data.Position.X);
            PlayerPrefs.SetFloat(XmlNameAttributes.PositionY, data.Position.Y);
            PlayerPrefs.SetFloat(XmlNameAttributes.PositionZ, data.Position.Z);
            PlayerPrefs.SetString(XmlNameAttributes.IsEnable, data.IsEnabled.ToString());

            PlayerPrefs.Save();
        }

        public void Clear()
        {
            PlayerPrefs.DeleteAll();
        }

        public void Clear(string value)
        {
            PlayerPrefs.DeleteKey(value);
        }

    }
}