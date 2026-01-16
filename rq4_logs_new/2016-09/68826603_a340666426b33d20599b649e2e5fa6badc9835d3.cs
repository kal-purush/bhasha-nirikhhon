using UnityEngine;
using System.Collections;

public class Dart_Script : MonoBehaviour {
	private Rigidbody rb;

	// Use this for initialization
	void Start () {
		rb = GetComponent<Rigidbody> ();
	}
	
	// Update is called once per frame
	void Update () {
		//Debug.Log (rb.velocity.y);
	}

	void OnCollisionEnter(Collision col) {
		if (col.gameObject.tag == "Enemy") {
			Destroy (col.gameObject);
			Destroy (this.gameObject);
		}
	}
}