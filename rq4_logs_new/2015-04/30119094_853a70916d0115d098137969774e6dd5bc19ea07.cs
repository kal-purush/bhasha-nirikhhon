using UnityEngine;
using System.Collections;

public class BikeCollider : MonoBehaviour {

	private bool rend;

	// Use this for initialization
	void Start () {
		rend = true;
	}
	
	// Update is called once per frame
	void Update () {
	
	}

	void OnCollisionStay(Collision col){
		if (col.gameObject.tag == "animal") {
			gameObject.GetComponent<SpriteRenderer>().enabled = rend;
			rend = !rend;
		}
	}

	void OnCollisionExit(Collision col){
		if (col.gameObject.tag == "animal") {
			rend = true;
		}
	}

}