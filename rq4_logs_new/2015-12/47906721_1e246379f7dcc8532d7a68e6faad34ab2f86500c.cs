using UnityEngine;
using System.Collections;

public class Generic_Control : MonoBehaviour {
	public bool isEnabled = false;
	Rigidbody2D rb;
	void Awake () {
		rb = GetComponent<Rigidbody2D> ();
	}

	public bool canSwitch () {
		return (rb.velocity.magnitude <= 0.01f);
	}
}