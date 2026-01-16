using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using System;
using UnityEngine.Networking.Match;
using UnityEngine.SceneManagement;
using UnityEngine.Networking;

public class NetworkManager : UnityEngine.Networking.NetworkManager
{
	public event Action<bool, MatchInfo> matchCreated;

	public event Action<bool, MatchInfo> matchJoined;

	private Action<bool, MatchInfo> NextMatchCreatedCallback;

	List<NetworkPlayer> players;

	public static NetworkManager Instance;

	int iActivePlayer = 0;
	public int ActivePlayer
	{
		get
		{
			return iActivePlayer;
		}
	}

	private void Awake()
	{
		if (!Instance)
		{
			Instance = this;
		}
	}
	// Use this for initialization
	void Start()
	{
		players = new List<NetworkPlayer>();
		SceneManager.sceneLoaded += OnSceneLoaded;
	}

	// Update is called once per frame
	void Update()
	{
		if (players.Count > 0)
		{
			CheckPlayersReady ();
		}
	}

	bool CheckPlayersReady()
	{
		bool playersReady = true;
		foreach (var player in players)
		{
			//playersReady &= player.ready;
			// legg inn en bool "ready" etter en spiller har satt opp bedriften sin
		}

		if (playersReady)
		{
			//players[iActivePlayer].StartGame();			skrives om, nå startes spillet
		}

		return playersReady;
	}

	public void ReTurn()
	{
		Debug.Log ("turn::"+iActivePlayer);		//
		players[iActivePlayer].TurnStart();
	}
	/*
	public void AlterTurns()
	{
		Debug.Log ("turn::"+iActivePlayer);

		players[iActivePlayer].TurnEnd();
		iActivePlayer = (iActivePlayer + 1) % players.Count;
		players[iActivePlayer].TurnStart();
	}
	*/
	// fungerer ikke slik vi vil ha det, rulerer mellom personer som tar sin tur. Skrives om.
	public void UpdateScore(int score)
	{
		//players [ActivePlayer].UpdateScore (score);
		//endres til update statistics
	}

	public void RegisterNetworkPlayer(NetworkPlayer player)
	{
		if (players.Count <= 2)
		{
			players.Add(player);
		}
	}

	public void DeregisterNetworkPlayer(NetworkPlayer player)
	{
		players.Remove(player);
	}

	public void CreateOrJoin(string gameName, Action<bool, MatchInfo> onCreate)
	{
		StartMatchMaker();
		NextMatchCreatedCallback = onCreate;
		matchMaker.ListMatches(0, 10, "turnbasedgame", true, 0, 0, OnMatchList);
	}

	public void OnSceneLoaded(Scene scene, LoadSceneMode mode)
	{
		if(scene.name == "GameScene")		// rename
		{
			NetworkServer.SpawnObjects();
		}
	}

	public override void OnMatchList(bool success, string extendedInfo, List<MatchInfoSnapshot> matches)
	{
		Debug.Log("Matches:" + matches.Count);
		if (success && matches.Count > 0)
		{
			matchMaker.JoinMatch(matches[0].networkId, string.Empty, string.Empty, string.Empty, 0, 0, OnMatchJoined);
		}
		else
		{
			CreateMatch("turnbasedgame");
		}
	}

	public void CreateMatch(string matchName)
	{
		matchMaker.CreateMatch(matchName, 2, true, string.Empty, string.Empty, string.Empty, 0, 0, OnMatchCreate);
	}

	public override void OnMatchCreate(bool success, string extendedInfo, MatchInfo matchInfo)
	{
		base.OnMatchCreate(success, extendedInfo, matchInfo);
		Debug.Log("OnMatchCreate"+matchInfo.networkId);

		// Fire callback
		if (NextMatchCreatedCallback != null)
		{
			NextMatchCreatedCallback(success, matchInfo);
			NextMatchCreatedCallback = null;
		}

		// Fire event
		if (matchCreated != null)
		{
			matchCreated(success, matchInfo);
		}
	}

	public override void OnMatchJoined(bool success, string extendedInfo, MatchInfo matchInfo)
	{
		base.OnMatchJoined(success, extendedInfo, matchInfo);
		Debug.Log("OnMatchJoined"+matchInfo.networkId);

		// Fire callback
		if (NextMatchCreatedCallback != null)
		{
			NextMatchCreatedCallback(success, matchInfo);
			NextMatchCreatedCallback = null;
		}

		// Fire event
		if (matchJoined != null)
		{
			matchJoined(success, matchInfo);
		}
	}
}