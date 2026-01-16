using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CameraShake : MonoBehaviour {

    public float shakeAmt;
    public float shakeTimerMax = 2;

    private float shakeTimer;

    private Vector3 originalPosition;

	// Use this for initialization
	void Start () {

        // Stat at THIS camera's position (transform.position)
        originalPosition = transform.position;

	}
	
	// Update is called once per frame
	void Update () {

        if (Input.GetKeyDown(KeyCode.S))
        {
            Shake();
        }
		
        if (shakeTimer > 0)
        {
            // Random.value returns a float between 0 and 1
            // Random.insideUintSphere adds a vector 
            transform.position = transform.position + (Random.insideUnitSphere * shakeAmt);
            shakeTimer -= Time.deltaTime;
        }
        else
        {
            transform.position = Vector3.Lerp(transform.position, originalPosition, .8f);
        }
	}

    public void Shake()
    {
        shakeTimer = shakeTimerMax;

    }
}