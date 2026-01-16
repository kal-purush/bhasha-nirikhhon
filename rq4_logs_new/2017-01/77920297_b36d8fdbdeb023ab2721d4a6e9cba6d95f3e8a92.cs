using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class GameLobbyManager : MonoBehaviour {

	private List<int> playerDifficulties = new List<int>() { 0, 1, 1, 1 };
	private List<string> playerNames = new List<string>() { "Human", "AI\nEasy", "AI\nNormal", "AI\nBoss" };

	// Use this for initialization
	void Start () {
		
	}
	
	// Update is called once per frame
	void Update () {
		
	}

	public void ChangePlayer (int playerId) {
		if (playerId < playerDifficulties.Count) {
			playerDifficulties [playerId] += 1;
			if (playerDifficulties [playerId] >= playerNames.Count) {
				playerDifficulties [playerId] = 0;
			}
				
			// now update the text
			GameObject curPlayerDiff = GameObject.Find ("Player" + (playerId+1));
			Debug.Log ("Player" + (playerId+1));
			curPlayerDiff.GetComponentInChildren<Text>().text = playerNames[playerDifficulties [playerId]];
		}
	}
}