using UnityEngine;
using System.Collections;
using System.Collections.Generic;

public class Referee : MonoBehaviour {

	private MultiplayerManager mm;
	public List<ActivePlayer> ingamePlayers;
	private int playersAlive = 0;

	// Use this for initialization
	void Start () {
		mm = GameObject.Find("Multiplayer Manager").GetComponent<MultiplayerManager>();
		if(mm) {
			foreach(MyPlayer mp in mm.PlayerList) {
				AddPlayer(mp.playerName, mp.playerNetwork);
			}
		} else {
			Debug.Log("ERROR! Could not find the multiplayer manager!");
		}
	}
	
	// Update is called once per frame
	void Update () {
	
	}

	void OnGUI() {
		if (GUI.Button(new Rect(Screen.width - 200, Screen.height - 200, 100, 100), "Win")) {
			MultiplayerManager.instance.GetComponent<NetworkView>().RPC("AssignWin", Network.player);
		}
	}

	public void KillPlayer(NetworkPlayer view) {
		foreach(ActivePlayer ap in ingamePlayers) {
			if (ap.playerNetwork == view) {
				ap.isAlive = false;
				--playersAlive;
			}
		}

		if(playersAlive == 1) {
			Debug.Log("Game over! One person left alive");
			foreach(ActivePlayer ap in ingamePlayers) {
				if (ap.isAlive) {
					Debug.Log("The winner is " + ap.playerName);
					mm.GetComponent<NetworkView>().RPC("AssignWin", ap.playerNetwork);
					return;
				}
			}
		}
	}

	void AddPlayer(string name, NetworkPlayer view) {
		ActivePlayer tempPlayer = new ActivePlayer();
		tempPlayer.playerName = name;
		tempPlayer.playerNetwork = view;
		tempPlayer.isAlive = true;
		ingamePlayers.Add(tempPlayer);
		++playersAlive;
	}
}

[System.Serializable]
public class ActivePlayer {
	public string playerName = "";
	public NetworkPlayer playerNetwork;
	public bool isAlive;
}