using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using Valve.VR;
using Valve.VR.InteractionSystem;

public class FloatBallScript : MonoBehaviour {

	[SerializeField]
	ButtonScript ballScript;

	[SerializeField]
	RackButtonScript pinScript;

	// Use this for initialization
	void Start () {
		
	}
	
	// Update is called once per frame
	void Update () {
		
	}
	void HandHoverUpdate(Hand hand){
		if (hand.GetStandardInteractionButtonDown () == true){
			ballScript.FloatBall ();
			pinScript.NormalPins ();
		}
	}
}