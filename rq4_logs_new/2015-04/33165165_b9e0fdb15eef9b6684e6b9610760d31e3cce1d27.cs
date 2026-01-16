using UnityEngine;
using System.Collections;
using System.Collections.Generic;
using UnityEngine.UI;
using System.IO;

public class MultiplayerManager : MonoBehaviour {
	
	public static MultiplayerManager instance;
	private string matchName = "";				//name of the match
	private string matchPassword = "";			//password to enter the match
	private int matchMaxUsers = 4;				//max players in the match
	
	public string playerName = "Host Player";	//the player name the current player will have in the match
	
	public List<MyPlayer> PlayerList = new List<MyPlayer>();
	public List<MapSettings> MapList = new List<MapSettings>();

	public MapSettings currentMap = null;

	public int oldPrefix;
	public bool isMatchStarted = false;
	
	// Use this for initialization
	void Start () {
		instance = this;
		playerName = PlayerPrefs.GetString("Player Name");	//get saved player settings for their name

		DirectoryInfo levelDir = new DirectoryInfo("Assets/Resources/Levels");
		FileInfo[] levelInfo = levelDir.GetFiles("*.*");

		foreach(FileInfo level in levelInfo)
		{
			if(level.Name.EndsWith(".xml"))
			{
				MapSettings newLevelDisplay = new MapSettings();
				newLevelDisplay.mapName = level.Name.Remove(level.Name.Length - 4);
				newLevelDisplay.mapLoadName = level.Name;
				MapList.Add(newLevelDisplay);
			}
		}

		if(MapList.Count > 0)
			currentMap = MapList[0];
	}

	//keep the instance alive
	void FixedUpdate() {
		instance = this;
	}
	
	// Start the server with the server name, max users, and a password (optional)
	public void StartServer (string serverName, string serverPassword, int maxUsers) {
		matchName = serverName;
		matchPassword = serverPassword;
		matchMaxUsers = maxUsers;
		
		Network.InitializeServer(matchMaxUsers, 2652, false);				//set the max number of players, port number, and something else
		MasterServer.RegisterHost("Deathmatch", matchName, "No Comment");	//gametype, game name, and comment

		//Network.InitializeSecurity();	//breaks everything till further notice
	}
	
	void OnServerInitialized() {
		Server_PlayerJoinRequest(playerName, Network.player);	//add the host to the list
	}
	
	void OnConnectedToServer() {
		GetComponent<NetworkView>().RPC("Server_PlayerJoinRequest", RPCMode.Server, playerName, Network.player); //add new player to server
	}
	
	void OnPlayerDisconnected(NetworkPlayer id) {
		GetComponent<NetworkView>().RPC("Client_RemovePlayer", RPCMode.All, id);	//remove disconnected player from server
	}

	void OnPlayerConnected(NetworkPlayer player) {
		//the newly connected player gets a list of all the other players
		foreach(MyPlayer pl in PlayerList) {
			GetComponent<NetworkView>().RPC("Client_AddPlayerToList", player, pl.playerName, pl.playerNetwork);
		}
		//tell the newly connected player the settings and map to load
		GetComponent<NetworkView>().RPC("Client_GetMultiplayerMatchSettings", player, currentMap.mapName, "", "");
	}

	void OnDisconnectedFromServer(NetworkDisconnection info) {
		PlayerList.Clear();
	}

	[RPC]
	void Server_PlayerJoinRequest(string playerName, NetworkPlayer view) {
		//tell everyone on server to add player to their list
		GetComponent<NetworkView>().RPC("Client_AddPlayerToList", RPCMode.All, playerName, view);
	}
	
	[RPC]
	//adds player to the player list with the input playername and networkplayer
	void Client_AddPlayerToList(string playerName, NetworkPlayer view) {
		MyPlayer tempPlayer = new MyPlayer();
		tempPlayer.playerName = playerName;
		tempPlayer.playerNetwork = view;
		PlayerList.Add(tempPlayer);
	}
	
	[RPC]
	//remove the plaer with the input networkplayer from the player list
	void Client_RemovePlayer(NetworkPlayer view) {
		MyPlayer tempPlayer = null;
		foreach(MyPlayer pl in PlayerList) {
			if(pl.playerNetwork == view) {
				tempPlayer = pl;
			}
		}
		
		if (tempPlayer != null) {
			PlayerList.Remove(tempPlayer);
		}
	}

	[RPC]
	void Client_GetMultiplayerMatchSettings(string map, string mode, string others) {
		currentMap = GetMap(map); //can add in mode later if I want to
	}

	//can possibly make better by getting rid of get variable. we will see if i need it later
	public MapSettings GetMap(string name) {
		MapSettings get = null;

		foreach(MapSettings st in MapList) {
			if(st.mapName == name) {
				get = st;
				return get;
			}

		}

		return get;
	}

	[RPC]
	void Client_LoadMultiplayerMap(string map, int prefix) {
		Network.SetLevelPrefix(prefix); //make sure you only recieve new RPCs
		Application.LoadLevel(map);
	}
	
	
}

[System.Serializable]
public class MyPlayer {
	public string playerName = "";
	public NetworkPlayer playerNetwork;
	public int skinID = 0;
	public int ammo = 4;
}

[System.Serializable]
public class MapSettings {
	public string mapName;
	public string mapLoadName;
	public Texture mapLoadTexture;
}