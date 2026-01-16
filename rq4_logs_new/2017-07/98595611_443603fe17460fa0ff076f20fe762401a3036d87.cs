
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class BatteryRotate : MonoBehaviour {

    public float bobHeight = 0.25f;
    public float bobFrequency = 0.5f;
    public float rotSpeed;
    private Vector3 offset;
    private Vector3 originalPos;

	// Use this for initialization
	void Start () {
        originalPos = transform.position;

    }
	
	// Update is called once per frame
	void Update () {
        offset.y = bobHeight * Mathf.Cos(bobFrequency * Time.time);
        transform.position = originalPos + offset;
        transform.Rotate(Vector3.up, rotSpeed * Time.deltaTime);

	}
}