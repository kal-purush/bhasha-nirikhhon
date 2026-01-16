using UnityEngine;
using System.Collections;

public class ConditionManager : MonoBehaviour {
	
	public GameObject ship;
	public GameObject[] enemyShips;

	public bool gameover;
	public bool won;
	public bool lost;

	public int day;
	public int oldDay;
	public int dayOfReckoning = 7;

	public Material[] mastParts;

	public static ConditionManager instance { get; private set; }

	void Awake ()
	{
		instance = this;
	
	}
	
	void Update () {
		if (DayNightController.daysPast > dayOfReckoning) {
			shipsAppear ();
			day = DayNightController.daysPast;
		}

		checkGameOver ();
		//updateMasts ();
	}

	public bool checkMorale() {
		if (DataValues.instance.totalMorale <= 0) {
			lost = true;
			return true;
		}
		return false;
	}

	public bool anyDaysLeft() {
		bool e = DayNightController.daysPast >= DataValues.instance.defaultDays;
		Debug.Log ("Checking Days: " + e + " " + DayNightController.daysPast + " / " + DataValues.instance.defaultDays);
		if (DayNightController.daysPast >= DataValues.instance.defaultDays) {
			if (DataValues.instance.repair >= DataValues.instance.maxRepair) {
				won = true;
				Debug.Log ("You won!");
			} else {
				lost = true;
				Debug.Log ("You lost!");
			}
			gameover = true;
			return true;
		}
		return false;
	}

	public void checkGameOver() {
		if (anyDaysLeft () || checkMorale ())
			gameover = true;
	}

	public void shipsAppear() {
		foreach (GameObject g in enemyShips)
			g.SetActive (true);
	}

//	public void updateMasts() {
//		if (DataValues.instance.repair / DataValues.instance.maxRepair > .15)
////			mastParts [0].color.a = 1f;
//		if (DataValues.instance.repair / DataValues.instance.maxRepair > .75)
////			mastParts [1].color.a = 1f;
//	}
}