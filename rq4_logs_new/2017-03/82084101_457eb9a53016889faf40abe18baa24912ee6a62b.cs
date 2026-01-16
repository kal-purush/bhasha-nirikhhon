/*
 * Erica Penniman
 * March 5, 2017
 * CS 3540
 * 
 * Makes the camera follow the mouse
 */

using System.Collections;
using UnityEngine;

public class CameraControl : MonoBehaviour
{
	public float speed = 30.0f;

	void Update () 
	{
		//if (Input.GetMouseButton (1)) {
			if (Input.GetAxis ("Mouse X") > 0) {
				transform.position += new Vector3 (Input.GetAxisRaw ("Mouse Y") * Time.deltaTime * -1 * speed, 
					0.0f, Input.GetAxisRaw ("Mouse X") * Time.deltaTime * speed);
			}

			else if (Input.GetAxis ("Mouse X") < 0) {
				transform.position += new Vector3 (Input.GetAxisRaw ("Mouse Y") * Time.deltaTime * -1 * speed, 
					0.0f, Input.GetAxisRaw ("Mouse X") * Time.deltaTime * speed);
			}
		//}
	}
}