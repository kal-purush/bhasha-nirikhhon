using UnityEngine;
using System.Collections;
using System.Collections.Generic;

public class GameManager : MonoBehaviour 
{
	[HideInInspector]public int roundNumber;
	[HideInInspector]public int commonMoney;
	[HideInInspector]public int playersAmount;
	[HideInInspector]public int playersRemaining;
	[HideInInspector]public int playersReady;
	[HideInInspector]public int chestMoney;
	List<Player> playerList = new List<Player>();

	void Start()
	{
		foreach(GameObject go in transform)
		{
			playerList.Add(go.GetComponent<Player>());
		}
	}

	void Update () 
	{
		if(playersRemaining == 0)
		{
			NewRound ();
		}

		Invoke("RandomEvent", 8f);
	}

	void RandomEvent()
	{
		float room = Random.Range(0, 1);
		if(room <= 0.33f)
		{
			TakeDamage();
		}
//		else if(room <= 0.50f)
//		{
//			FindChest();
//		}
		else
		{
			FindMoney ();
		}
	}

	void FindMoney()
	{
		int money;
		float randomMoney = Random.Range(0, 1);
		if(randomMoney <= 0.1f)
		{
			money = 50;
		}
		else if(randomMoney <= 0.30f)
		{
			money = 20;
		}
		else if(randomMoney <= 0.70f)
		{
			money = 10;
		}
		else if(randomMoney <= 0.90f)
		{
			money = 5;
		}
		else
		{
			money = 0;
		}

		for(int i = 0; i < playerList.Count; i++)
		{
			playerList[i].currentMoney += money/playersRemaining;
			commonMoney += money % playersRemaining;
			print (playerList[i].playerName + " got " + money/playersRemaining + "and now has " + playerList[i].currentMoney + " on him.");
		}
		print ("There is now " + commonMoney + " in the stash.");
	}
	
//	static void FindChest()
//	{
//
//	}

	void TakeDamage()
	{
		int damage;
		float randomDamage = Random.Range(0, 1);
		if(randomDamage <= 0.33f)
		{
			damage = 2;
		}
		else
		{
			damage = 1;
		}

		for(int i = 0; i < playerList.Count; i++)
		{
			playerList[i].hitPoints -= damage;
		}
	}

	void NewRound()
	{
		for(int i = 0; i < playerList.Count; i++)
		{
			playerList[i].hitPoints = 2;
		}
		commonMoney = 0;
		roundNumber++;
	}
}