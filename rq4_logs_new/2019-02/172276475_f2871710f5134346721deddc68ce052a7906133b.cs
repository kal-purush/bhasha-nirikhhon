using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class DS_MoveStair : MonoBehaviour {

	public float Speed;
	
	// Update is called once per frame
	void Update () {
		transform.Translate (Vector2.up * Time.deltaTime);
	}

	void OnCollisionEnter2D(Collision2D coll)
	{
		
		if (coll.gameObject.CompareTag ("top")) {
			StartCoroutine (waitDistroy ());
		}
	}

	IEnumerator waitDistroy()
	{
		yield return new WaitForSeconds (1f);
		Destroy (this.gameObject);
	}
}