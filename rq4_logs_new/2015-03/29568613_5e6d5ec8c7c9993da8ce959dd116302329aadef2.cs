using UnityEngine;
using System.Collections;

public class ConditionManager : MonoBehaviour {

	public GameObject ship;
	public GameObject[] enemyShips;
	public bool finalCountdown;
	public int day;
	public int oldDay;

	public static ConditionManager instance { get; private set; }

	void Awake ()
	{
		instance = this;
	
	}
	
	void Update () {
		if (DayNightController.daysPast > 20) {
			shipsAppear ();
			day = DayNightController.daysPast;
		}
		if (finalCountdown && day > oldDay) {
			moveShips ();
			oldDay = day;
		}
	}

	public void shipsAppear() {
		foreach (GameObject g in enemyShips)
			g.SetActive (true);
		finalCountdown = true;
	}

	public void moveShips() {
		foreach (GameObject g in enemyShips)
			g.transform.position += Vector3.Distance (g.transform.position, ship.transform.position)
				/ 10 * new Vector3 (-1, 0, -1);
	}
}