using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class SwitchInteract : MonoBehaviour {

	public GameObject currentObj = null;

	void OnTriggerEnter2D(Collider2D other) 
	{
		if (other.CompareTag ("Switch")) 
		{
			Debug.Log (other.name);
			currentObj = other.gameObject;
			currentObj.SendMessage ("TurnSwitchOn");
		}
	}

	void OnTriggerExit2D(Collider2D other)
	{
		if (other.CompareTag ("Switch")) 
		{
			currentObj.SendMessage ("TurnSwitchOff");

			if (other.gameObject == currentObj)
				currentObj = null;
		}
	}
}