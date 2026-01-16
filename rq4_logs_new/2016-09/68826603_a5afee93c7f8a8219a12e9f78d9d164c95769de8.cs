using UnityEngine;
using System.Collections;

public class TutorialEnemy : MonoBehaviour {

	Vector3 origPos;
    MainPlayer player;


	void Start () {
        origPos = transform.position; //where the enemy spawns, not ready yet
        tag = "Enemy";
        player = GameObject.Find("Player").GetComponent<MainPlayer>();
	}
	
	void FixedUpdate () {
		//always rotates to face player
		transform.rotation = Quaternion.LookRotation(
            player.transform.position - transform.position);
	}
}