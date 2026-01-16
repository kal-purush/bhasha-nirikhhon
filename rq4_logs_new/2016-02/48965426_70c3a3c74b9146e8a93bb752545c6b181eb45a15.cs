using UnityEngine;
using System.Collections;

public class MoveCar : MonoBehaviour {

	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
        gameObject.transform.position = gameObject.transform.position + new Vector3(0,0,0.01f);
	}
}