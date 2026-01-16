using UnityEngine;
using System.Collections;

public class BlinkMechanic : MonoBehaviour {

	public float blinkDistance;

	// Update is called once per frame
	void Update ()
	{
		if (Input.GetButtonDown ("Blink")) {
			Debug.Log("Blinked!");
			RayCastBlink();
		}
	}

	void RayCastBlink ()
	{
		RaycastHit hit;
		Vector3 position = transform.position;
		Vector3 forward = transform.forward;
		Ray blinkRay = new Ray(position, forward);
		if(Physics.Raycast(blinkRay, out hit, blinkDistance)) // if our blink ray collides with something
		{
			Debug.DrawRay(position, forward * blinkDistance, Color.red, 10f, true);
			transform.position = hit.point + hit.normal;
		}
		else
		{
			Debug.DrawRay(position, forward * blinkDistance, Color.blue, 10f, true);
			transform.position = position + ( forward * blinkDistance);
		}

	}

}