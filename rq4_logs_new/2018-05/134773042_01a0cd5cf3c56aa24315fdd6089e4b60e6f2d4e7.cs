using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Orbit : MonoBehaviour {

    public Transform centralObject;
    public float centralMass;

    public Rigidbody myBody;
    public float myMass;

    public float initForce = 100.0f;

    public float distSqr = 0;
    public float speed;

	// Use this for initialization
	void Start () {
        myBody = GetComponent<Rigidbody>();
        myBody.AddForce(transform.right * initForce);

        centralMass = centralObject.GetComponent<Rigidbody>().mass;
        myMass = myBody.mass;

	}
	
	// Update is called once per frame
	void FixedUpdate () {
        Vector3 inward = centralObject.position - transform.position;
        distSqr = inward.sqrMagnitude;
        inward.Normalize(); // get direction only

        myBody.AddForce(inward * (centralMass + myMass) / distSqr);

        speed = myBody.velocity.magnitude;
	}
}