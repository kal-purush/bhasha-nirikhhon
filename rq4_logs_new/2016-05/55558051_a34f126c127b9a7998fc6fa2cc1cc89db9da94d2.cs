using UnityEngine;
using System.Collections;

public class BounceObjectt : MonoBehaviour {

	PointsGenerator script;
	public Transform PointsTriggertransform;
	// Use this for initialization
	void Start () {
		script = FindObjectOfType<PointsGenerator> ();
	}
	
	// Update is called once per frame
	void Update () {
	
	}

	void OnCollisionEnter(Collision collision) {
		script.PointDesing (PointsTriggertransform , "OUCH!!!");
	}
}