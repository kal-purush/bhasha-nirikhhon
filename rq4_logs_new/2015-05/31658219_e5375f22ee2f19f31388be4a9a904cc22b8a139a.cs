using UnityEngine;
using System.Collections;

public class Rotate : MonoBehaviour {

	public float speed = 50f;
	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
		transform.RotateAround(Vector3.zero, Vector3.up, speed * Time.deltaTime);
	}
}