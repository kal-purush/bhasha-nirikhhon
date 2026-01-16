using System;
using System.Collections.Generic;
using System.Xml;
using UnityEngine;

public class LanguageController : MonoBehaviour
{
    public Language CurrentLanguage { get; private set; }
    public Dictionary<string, string> LocalizationStrings { get; private set; }

    public void Awake()
    {
        DontDestroyOnLoad(gameObject);
        if (PlayerPrefs.HasKey("Language"))
            SetLanguage(PlayerPrefs.GetInt("Language"));
        else
            SetLanguage(GetSystemLanguage());
    }

    private Language GetSystemLanguage(Language DefaultLanguage = Language.English)
    {
        return Enum.IsDefined(typeof(Language), (int)Application.systemLanguage) ? DefaultLanguage : (Language)Application.systemLanguage;
    }

    private XmlNodeList LoadLocalizationFile(string fileName)
    {
        XmlDocument localizationFile = Services.FileService.LoadXml($"LocalizationFiles/{fileName}.xml");
        return localizationFile.SelectNodes($"{fileName}/string");
    }

    public void SetLanguage(Language language)
    {
        // If the language is not defined, trying to load could create many errors
        if (!Enum.IsDefined(typeof(Language), language))
            throw new ArgumentException("I don't speak Klingon");
        CurrentLanguage = language;
        // TODO replace this by LogMessage
        Debug.Log($"DEBUG - 22 | CurrentLanguage: {CurrentLanguage}");
        // Save this value for the next time the app is used
        PlayerPrefs.SetInt("Language", (int)CurrentLanguage);
        LocalizationStrings = new Dictionary<string, string>();
        foreach (string fileName in new string[] { "General", CurrentLanguage.ToString() })
        {
            foreach (XmlNode node in LoadLocalizationFile(fileName))
            {
                // "name" is the attribute containing the localization string in the xml file
                LocalizationStrings.Add(node.Attributes["name"].Value, node.InnerText);
            }
        }
    }

    public void SetLanguage(int languageIndex)
    {
        SetLanguage((Language)languageIndex);
    }

    public string GetText(string localizationString)
    {
        if (!LocalizationStrings.ContainsKey(localizationString))
        {
            // If there is no translation available
            Debug.LogWarning($"WARNING: Missing translation for {localizationString}");
            return localizationString;
        }
        else
            //
            return LocalizationStrings[localizationString];
    }

    public string GetLocalizationString(string text)
    {
        foreach (KeyValuePair<string, string> pair in LocalizationStrings)
        {
            if (pair.Value == text)
                return pair.Key;
        }
        return null;
    }
}