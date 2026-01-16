using UnityEngine;
using System.Collections;

public class Bounce : MonoBehaviour {

	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update ()
	{
	    transform.position = new Vector3(0, Mathf.Sin(Time.time) + 2,0);
	}
}