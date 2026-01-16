using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class AstroidBehavior : MonoBehaviour {

    private float rotationIntesity;
    private float direction;

    // Use this for initialization
    void Start()
    {
        direction = Random.Range(0.0f, 3.0f);
        rotationIntesity = Random.Range(0.0f, 10.0f);
	}
	
	// Update is called once per frame
	void Update ()
    {
		if (direction <= 1.0f)
            transform.Rotate(rotationIntesity * Time.deltaTime, 0.0f, 0.0f);
        else if (direction <= 2.0f)
            transform.Rotate(0.0f, rotationIntesity * Time.deltaTime, 0.0f);
        else if (direction <= 3.0f)
            transform.Rotate(0.0f, 0.0f, rotationIntesity * Time.deltaTime);
	}
}