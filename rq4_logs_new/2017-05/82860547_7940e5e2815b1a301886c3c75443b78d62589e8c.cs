using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Trap : MonoBehaviour {
	public GameObject monster;
	public Camera camera;
	public bool summoned;
	public bool canSummon;

	// Use this foinitialization
	void Start () {
		summoned = false;
		canSummon = false;
	}

	// Update is called once per frame
	void Update () {

	}

	public void OnMouseUp()
	{

		if (canSummon)
		{
			if (!summoned)
			{
				if (camera.ScreenToWorldPoint(Input.mousePosition).x < 0)
				{

					Debug.Log("Player Trap");
					GameObject unit = PhotonNetwork.Instantiate("playertrap", this.transform.position, monster.transform.rotation, 0);
					unit.GetComponent<Unit>().master = 1;
					GameLists.PlayerTrap.Add(unit);
					summoned = true;
					GameLists.SummonZones.Add(this.gameObject);
					foreach (GameObject zone in GameLists.PlayerTrapSummon)
					{
						Debug.Log(zone.name + "trying to see if false");
						if (zone.GetComponent<Trap>().canSummon)
						{
							zone.GetComponent<Trap>().canSummon = false;
						}
					}


				}
				else
				{
					Debug.Log("Enemy Trap");
					GameObject unit = PhotonNetwork.Instantiate("enemytrap", this.transform.position, monster.transform.rotation, 0);
					GameLists.EnemyTrap.Add(unit);
					unit.GetComponent<Unit>().master = 2;
					summoned = true;
				}
			}
		}
	}

	public void resetSummon()
	{
		summoned = false;
	}
}