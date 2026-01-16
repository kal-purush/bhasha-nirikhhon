using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class playerStatistics : MonoBehaviour {

	public int userbase;
	public int money;
	public int publicOpinion;
	public string name;


	// Use this for initialization
	void Start () {
		userbase = 10;
		money = 10;
		publicOpinion = 0;
	}
	
	// Update is called once per frame
	void Update () {
		//	nothing
	}

	public void updateUserbase(int newUsers){
		userbase = userbase + newUsers;
	}

	public void updateMoney(int income){
		money = money + income;
	}

	public void updatePublicOpinion(int opinion){
		publicOpinion = publicOpinion + opinion;
	}
}