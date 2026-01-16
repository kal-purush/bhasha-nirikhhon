using UnityEngine;
using System.Collections;

public class starCollision : MonoBehaviour {

	void OnTriggerEnter(Collider myCol){
		if (myCol.gameObject.tag == "Fighter") {
			myCol.gameObject.GetComponent<Health>().addDamage(-100);
		}

	}
}