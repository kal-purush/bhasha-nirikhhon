using UnityEngine;
using System.Collections;

public class Follow : MonoBehaviour {

	public Transform player; //the enemy's target
	public int moveSpeed = 3; //move speed
	
	void Start()
	{
		player = GameObject.FindWithTag("Player").transform; //target the player
	}
	
	
	void Update () {
		if (Vector3.Distance (player.transform.position, transform.position) > 1.5) {
			float step = moveSpeed * Time.deltaTime;
			//move towards the player
			this.transform.position = Vector3.MoveTowards (transform.position, player.position, step);
		}
		
	}
}