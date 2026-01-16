using UnityEngine;
using System.Collections;

public class ClickMove : MonoBehaviour 
{
	private Vector3 newPosition;

	void Awake () {
		newPosition = transform.position;
	}

	void Update () {
		if (Input.GetMouseButtonDown(0)){ // if left button pressed...
			Ray ray = GetComponent<Camera>().ScreenPointToRay(Input.mousePosition);
			RaycastHit hit;
			if (Physics.Raycast(ray, out hit)){
				// the object identified by hit.transform was clicked
				newPosition = hit.transform.position;
				newPosition += Vector3.up*(hit.transform.localScale.z/2f + 100f);
			}
		}
		transform.position = Vector3.Lerp (transform.position, newPosition, Time.deltaTime);
	}
}