using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class GameManager : MonoBehaviour {

	//creates a zombie and a player based off those classes
	//zombie attacks player
	void Start () {
		zombie zombie = new zombie ();
		Player player = new Player ();

		player.TakeDamage(zombie.Attack ());
		player.TakeDamage (zombie.AcidPukeAttack ());

		//or create a ton of zombies

		zombie[] zombies = new zombie[100];

		for (int x = 0; x < zombies.Length; x++) {
			zombies [x] = new zombie ();
			Debug.Log ("created zombie " + x);
		}
	}
	
	// Update is called once per frame
	void Update () {
		
	}
}