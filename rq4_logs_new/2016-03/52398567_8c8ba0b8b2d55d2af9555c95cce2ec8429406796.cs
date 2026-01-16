using UnityEngine;
using System;
using System.Collections;

public class Forog : MonoBehaviour {

    public Vector3 rotation;
    public float bounce;
    public float bounceFrequency;

    private Vector3 location;

    void Start()
    {
        location = transform.position;
    }


	// Update is called once per frame
	void FixedUpdate () {
        Vector3 rot = Time.fixedDeltaTime * rotation;
        transform.Rotate(rot);

        Vector3 pos = location;
        pos.y += bounce * (float) Math.Sin(bounceFrequency * Time.time);
        transform.position = pos;
    }
}