using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ArcadeGM : MonoBehaviour {
	public int Score = 0;//score of player
	public float Time = 0;//Time of player
	public int ComboCount = 0;//player's combo count 
	public GameObject EnermyObject;
	public int Money = 10000;//moeny of player

	void Start () {
	}
	public void SetTime()
	{
		this.Time += UnityEngine.Time.deltaTime;
	}
	void Update () {
		SetTime ();

		//test
		if(Input.GetKeyDown(KeyCode.A) )
			{
			AddScore (10);
			Debug.Log ("add 10 point by script get keycode A");
			}
		if(Input.GetKeyDown(KeyCode.S) )
		{
			ComboCountUP ();
			Debug.Log ("add Combo by script get keycode S");
		}
		if(Input.GetKeyDown(KeyCode.D) )
		{
			ComboCountInit ();
			Debug.Log ("combo init by script get keycode d");
		}
	}

	public void MakeEnemy()//create new enermy
	{
		Instantiate (EnermyObject);// (EnermyObject, new Vector3(Random.Range(0,Screen.width), 480, 0), Quaternion.identity);
	}

	public void AddScore(int addscore)//add player's score that i want score(example player score is 0 and AddScore(10) => player score is 10)
	{
		this.Score += addscore;
	}

	public void ComboCountUP()//player's combocount up (example 0 => 1)
	{
		this.ComboCount++;
	}

	public void ComboCountInit()//set player's combocount zero(example 3 => 0)
	{//player fail the combo, make combo count zero
		this.ComboCount = 0;
	}
}