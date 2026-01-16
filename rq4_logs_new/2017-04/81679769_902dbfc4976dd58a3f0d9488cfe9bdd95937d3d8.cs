/* 
 * This file is a template for our actual save and load system. We'll replace what the data that is being saved with 
 * what's actually in our game.
 */

using UnityEngine;
using System.Collections;
using System.Collections.Generic; 
using System.Runtime.Serialization.Formatters.Binary; 
using System.IO;

// There is no need for Monobehavior, therefore it's ommitted.
public static class SaveLoad {
	// The object, "savedGames" is the list of games that have been saved.
	public static List<Game> savedGames = new List<Game>();

	public static void Save() {
		// Append the current "Game" object to the list of saved games.
		savedGames.Add(Game.current);
		// Handles serialization work.
		BinaryFormatter bf = new BinaryFormatter();
		// Creates a filestream and a file object and passes it to the "saved games.gd location
		// where savedGames is the name of the location and .gd is the file type of the file location.
		FileStream file = File.Create (Application.persistentDataPath + "/savedGames.gd");	
		// Uses the Binary Formatter to create the file location as input.
		bf.Serialize(file, SaveLoad.savedGames);
		// Close the file.
		file.Close();
	}

	public static void Load() {
		// Checks if the game to load exists.
		if(File.Exists(Application.persistentDataPath + "/savedGames.gd")) {
			// Handles serialization work.
			BinaryFormatter bf = new BinaryFormatter();
			// Accces the filestream.
			FileStream file = File.Open(Application.persistentDataPath + "/savedGames.gd", FileMode.Open);
			// "bf.Deserialize(file) finds our file at the location and deserializes it.
			// (List<Game>)bf converts binary to type Game.
			SaveLoad.savedGames = (List<Game>)bf.Deserialize(file);
			// Close the file.
			file.Close();
		}
	}
}