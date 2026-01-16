using UnityEngine;
using System.Collections;

public class DetectDirection : MonoBehaviour {

	public string Dir;

	// Use this for initialization
	void Start () {

	}

	// Update is called once per frame
	void Update () {

	}

	void OnCollisionEnter(Collision col)
	{
		if (col.gameObject.tag == "LeftDir")
			Dir = "Left";
		else if (col.gameObject.tag == "RightDir")
			Dir = "Right";
	}
}