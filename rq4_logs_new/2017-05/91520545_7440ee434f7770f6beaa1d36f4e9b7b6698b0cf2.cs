using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class BattleManager : MonoBehaviour {

	IPlayerAction playerAction;
	public int homeLife = 10;
	public int visitorLife = 10;
	public string homeName = "Anonymous1";
	public string visitorName = "Anonymous2";

	public void Execute(){
		playerAction.Execute(this.gameObject, 10);
	}

	public void InitialHomeState(int playerLife,string playerName){
		this.homeLife = playerLife;
		this.homeName = playerName;
	}

	public void InitialVisitorState(int enemyLife,string enemyName){
		this.visitorLife = enemyLife;
		this.visitorName = enemyName;
	}

	public void SetExecution(IPlayerAction playerAction)
	{
		this.playerAction = playerAction;
	}
}