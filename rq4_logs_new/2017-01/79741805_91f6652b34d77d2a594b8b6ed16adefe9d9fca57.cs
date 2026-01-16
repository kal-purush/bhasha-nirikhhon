using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class FlipperScript : MonoBehaviour {



	public HingeJoint2D LeftFlipperHinge; 
	public JointMotor2D LeftFlipperHingeMotor; 

	// Use this for initialization
	void Start () {
		LeftFlipperHinge = gameObject.GetComponent<HingeJoint2D> ();
		LeftFlipperHingeMotor = LeftFlipperHinge.motor;
	}
	
	// Update is called once per frame
	void Update () {

		if (Input.GetKeyDown (KeyCode.A)) { 
			print ("A is being pressed");
			LeftFlipperHinge.useMotor = true;

		} 

		if (gameObject.transform.rotation.z > 0) { 

			LeftFlipperHinge.useMotor = false;

		} 

	}


}