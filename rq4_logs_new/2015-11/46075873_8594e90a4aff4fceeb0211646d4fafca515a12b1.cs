using UnityEngine;
using System.Collections;

public class RotateArmL : MonoBehaviour {

	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
		if (Input.GetKeyDown(KeyCode.W)) {
			transform.Rotate (new Vector3 ( 0f, 0f, 15f));
		}
		if (Input.GetKeyDown(KeyCode.S)) {
			transform.Rotate (new Vector3 ( 0f, 0f, -15f));
		} 
//		if (Input.GetKeyDown(KeyCode.A)) {
//			transform.Rotate (new Vector3 ( 0f, 15f, 0f));
//		} 
//		if (Input.GetKeyDown(KeyCode.S)) {
//			transform.Rotate (new Vector3 ( 15f, 0f, 0f));
//		} 
	}
}