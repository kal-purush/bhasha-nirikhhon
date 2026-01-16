using System.Collections.Generic;
using System.IO;
using GitHub.Unity.Json;

public abstract class Localization
{
	public static string Language { get; set; } = "Español";

	public static Dictionary<string, Dictionary<string, Dictionary<string, string>>> Messages =
		new Dictionary<string, Dictionary<string, Dictionary<string, string>>>();

	public static void LoadMessages()
	{
		if (Messages.Count == 0)
		{
			const string MESSAGES_PATH = "Assets/Resources/LanguageFiles/Messages.json";
			using (StreamReader reader = new StreamReader(MESSAGES_PATH))
			{
				string contents = reader.ReadToEnd();
				Messages = SimpleJson
					.DeserializeObject<
						Dictionary<string, Dictionary<string, Dictionary<string, string>>>>(
						contents);
			}
		}
	}

	public static string GetMessage(string scene, string item)
	{
		if (Messages.Count == 0)
		{
			LoadMessages();
		}

		return Messages[scene][item][Language];
	}
}