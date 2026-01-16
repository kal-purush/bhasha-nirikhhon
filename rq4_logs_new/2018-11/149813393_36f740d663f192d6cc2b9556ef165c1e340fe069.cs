using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class NeedleScript : MonoBehaviour {

	static float minAngle = -255.0f;
	static float maxAngle = -465.0f;
	static NeedleScript thisMental;

	// Use this for initialization
	void Start () {
		thisMental = this;
	}
	
	public static void ShowMental(float fill, float min, float max) {
		float ang = Mathf.Lerp(minAngle, maxAngle, Mathf.InverseLerp(min, max, fill));
		thisMental. transform.eulerAngles = new Vector3(0,0,ang);
	}
}