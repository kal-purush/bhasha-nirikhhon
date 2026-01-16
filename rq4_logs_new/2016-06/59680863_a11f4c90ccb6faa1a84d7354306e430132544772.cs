using UnityEngine;
using System.Collections;

public class FuelPickup : MonoBehaviour
{
	public float value;

	void Start ()
	{
	
	}

	void Update ()
	{
	
	}

	void OnTriggerEnter (Collider other)
	{
		Debug.Log ("hitttsss");

		if (other.transform.tag == "Player")
		{
			Debug.Log ("hit");
			other.GetComponent<PlayerController> ().boostFuel += value;

			if (other.GetComponent<PlayerController> ().boostFuel == other.GetComponent<PlayerController> ().maxBoostFuel)
				other.GetComponent<PlayerController> ().boostFuel = other.GetComponent<PlayerController> ().maxBoostFuel;
		}

		Destroy (this.gameObject);
	}
}