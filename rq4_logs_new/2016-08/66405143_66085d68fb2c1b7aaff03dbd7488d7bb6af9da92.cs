using UnityEngine;
using System.Collections;

public class SpintCube : MonoBehaviour {

	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
		gameObject.transform.Rotate (5, 0, 0);
	
	}
}