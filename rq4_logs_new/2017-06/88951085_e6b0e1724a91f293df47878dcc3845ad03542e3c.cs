using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class itemGet : MonoBehaviour {
	public GameObject player;

	// Use this for initialization
	void Start () {
		
	}
	
	// Update is called once per frame
	void Update () {
		
		//if (this.transform.position.x <= player.transform.position.x && this.transform.position.x + 3 >= player.transform.position.x && player.transform.position.y >= this.transform.position.y) {
		//	this.gameObject.SetActive (false);
		//	print ("get");
		//}
	}
	void OnCollisionEnter2D(Collision2D col){
		if (col.gameObject == player) {
			this.gameObject.SetActive (false);
			print ("get");
		}
	}
}