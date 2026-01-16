using System.Collections.Generic;
using System.IO;
using GitHub.Unity.Json;

public abstract class UserConfiguration
{
	public static string Language = "Español";
	public static string MusicVolume = "100";
	public static string FXSVolume = "100";

	private const string SETTINGS_PATH =
		"Assets/Resources/UserConfigurationFiles/UserConfiguration.json";

	public static void LoadSettings()
	{
		Dictionary<string, string> settings =
			SimpleJson.DeserializeObject<Dictionary<string, string>>(SETTINGS_PATH);
		Language = settings["Language"];
		MusicVolume = settings["MusicVolume"];
		FXSVolume = settings["FSXVolume"];
	}

	public static void SaveSettings()
	{
		Dictionary<string, string> settings = new Dictionary<string, string>();
		settings.Add("Language", Language);
		settings.Add("MusicVolume", MusicVolume);
		settings.Add("FSXVolume", FXSVolume);
		string content = SimpleJson.SerializeObject(settings);
		StreamWriter writer = new StreamWriter(SETTINGS_PATH, false);
		writer.Write(content);
		writer.Close();
	}
}