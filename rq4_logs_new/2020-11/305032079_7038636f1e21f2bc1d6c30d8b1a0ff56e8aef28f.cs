using System;
using System.Collections.Generic;
using System.IO;
using GitHub.Unity.Json;
using UnityEngine;

public static class MainConfiguration
{
	public static Dictionary<string, string> Settings { get; set; } =
		new Dictionary<string, string>();

	private const string MAIN_SETTINGS_PATH =
		"Assets/Resources/MainConfiguration/MainConfiguration.json";

	private static void LoadSettings()
	{
		string content = new StreamReader(MAIN_SETTINGS_PATH).ReadToEnd();
		Settings = SimpleJson.DeserializeObject<Dictionary<string, string>>(content);
	}

	public static bool SettingExists(string settingName)
	{
		if (Settings.Count == 0)
		{
			LoadSettings();
		}
		return Settings.ContainsKey(settingName);
	}

	public static string GetSetting(string settingName)
	{
		string setting = null;
		if (SettingExists(settingName))
		{
			setting = Settings[settingName];
		}
		return setting;
	}
}