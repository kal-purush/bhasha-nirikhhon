using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Interactable : MonoBehaviour {
	
	private Rigidbody rb;
	private bool pressed = false;
	private bool released = false;
	private Vector3 screenPoint;
	private Vector3 offset;
	private Vector3 oldPosition;

	// Use this for initialization
	void Start () {
		
	}
	
	// Update is called once per frame
	void Update () {
		if (!GlobalVariables.isPaused && !pressed) {
			while (transform.position.y < -75) {
				transform.position = new Vector3 (
					transform.position.x,
					transform.position.y + 10,
					transform.position.z
				);
			}
		}
	}

	void OnMouseDown(){

		if (!GlobalVariables.isPaused) {

			pressed = true;

			screenPoint = Camera.main.WorldToScreenPoint (gameObject.transform.position);

			offset = gameObject.transform.position - Camera.main.ScreenToWorldPoint (new Vector3 (Input.mousePosition.x, Input.mousePosition.y, screenPoint.z));

		}
	}

	void OnMouseDrag(){

		if (!GlobalVariables.isPaused) {

			oldPosition = transform.position;

			Vector3 curScreenPoint = new Vector3 (Input.mousePosition.x, Input.mousePosition.y, screenPoint.z);

			Vector3 curPosition = Camera.main.ScreenToWorldPoint (curScreenPoint); //&#43; offset;

			transform.position = curPosition + offset;
		}

	}

	void OnMouseUp(){
		if (!GlobalVariables.isPaused) {
			pressed = false;
			released = true;

			//rb.velocity = (transform.position - oldPosition) * Time.deltaTime * 5;
		}
	}
}