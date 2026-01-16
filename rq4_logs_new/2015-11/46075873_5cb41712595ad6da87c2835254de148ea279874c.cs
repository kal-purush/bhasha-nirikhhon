using UnityEngine;
using System.Collections;

public class arrowKeysMovement : MonoBehaviour {
	
	// Use this for initialization
	void Start () {
		
	}
	
	// Update is called once per frame
	void Update () {
		if (Input.GetKey (KeyCode.UpArrow)) {
			transform.Translate(new Vector3(0f, 0f, 10f * Time.deltaTime)); 
			
		}
		if (Input.GetKey (KeyCode.DownArrow)) {
			transform.Translate (new Vector3(0f, 0f, -10f * Time.deltaTime)); 
			
		}
		if (Input.GetKey (KeyCode.LeftArrow)) {
			transform.localEulerAngles += new Vector3(0f, -10f, 0f);
			
		}
		if (Input.GetKey (KeyCode.RightArrow)) {
			transform.localEulerAngles += new Vector3(0f, 10f, 0f); 
			
		}
	}
}