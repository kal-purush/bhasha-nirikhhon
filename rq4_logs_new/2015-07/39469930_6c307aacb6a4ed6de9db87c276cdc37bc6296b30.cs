using UnityEngine;
using System.Collections;

public class PortalObject: MonoBehaviour {

	public GameObject portalExit;
	public int verticalModifier;

	void OnTriggerEnter2D(Collider2D other)
	{
		if(other.gameObject.tag == "Player")
		{
			Debug.Log ("do portal things");
			Vector3 hitPoint = other.transform.position;
			Vector3 difference = transform.position - hitPoint;
			Vector3 correctedDiff = new Vector3(difference.x, 0f, 0f);
			other.transform.position = ((portalExit.transform.position - correctedDiff) + verticalModifier * (Vector3.up * (other.transform.localScale.y /2 + transform.localScale.y / 2)));
		}
	}

}