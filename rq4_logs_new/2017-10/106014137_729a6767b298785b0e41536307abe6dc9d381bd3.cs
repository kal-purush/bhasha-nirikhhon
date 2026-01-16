using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class kolizja : MonoBehaviour {

	void OnTriggerEnter2D(Collider2D node){
		if(node.gameObject.tag == "Wall")
		{
			Destroy(gameObject);
		}
	}
}