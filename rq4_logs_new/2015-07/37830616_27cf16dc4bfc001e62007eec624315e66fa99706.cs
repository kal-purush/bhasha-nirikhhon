using UnityEngine;
using System.Collections;
using System.Runtime.Serialization.Formatters.Binary;
using System.IO;

public class SaveLoad : MonoBehaviour {
	private static string FILE_PATH = Application.persistentDataPath + "/game.dat";

	public static void Save(GameData gameData)
	{
		BinaryFormatter bf = new BinaryFormatter ();
		FileStream file = File.Create(FILE_PATH);

		bf.Serialize (file, gameData);
		file.Close();
	}

	public static GameData Load()
	{
		GameData gameData = null;
		if (File.Exists (FILE_PATH)) 
		{
			BinaryFormatter bf = new BinaryFormatter ();
			FileStream file = File.Open(FILE_PATH, FileMode.Open);
			gameData = (GameData) bf.Deserialize(file);
			file.Close();
		}
		return gameData;
	}
}