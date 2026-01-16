using UnityEngine;
using System.Collections;

public class BeamWeapon : MonoBehaviour {

	private float beamLife = 0.5f;
	private float beamSpeed = 2f;

	// Use this for initialization
	void Start () {


		gameObject.GetComponent<Rigidbody2D> ().velocity.x = beamSpeed;
	
	}
	
	// Update is called once per frame
	void Update () {
	
	}
}