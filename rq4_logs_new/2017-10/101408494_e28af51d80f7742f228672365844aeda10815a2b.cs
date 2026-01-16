using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class AddPlayerToList : MonoBehaviour {

	// Use this for initialization
	public GameObject playerPrefab;
	public Transform playerList;

	public void addPlayer(){
		string newPlayerName = getNewPlayerName();
		changePrefabPlayerName(newPlayerName);
		Instantiate(playerPrefab, playerList);
	}

	private string getNewPlayerName(){
		GameObject playerNameGO = GameObject.Find("PlayerName");
		return playerNameGO.GetComponent<Text>().text;
	}

	void changePrefabPlayerName(string name){
		Transform input = playerPrefab.transform.GetChild(0);
		Text playerName = input.GetComponentInChildren<Text>();
		playerName.text = name;
	}
}