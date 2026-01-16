using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class WeaponSway : MonoBehaviour {

    public float swayAmount = 5;
    public float swaySpeed = 5;
    private Vector2 mouseDelta;
    private Vector3 weaponDelta;
    private Vector3 startPos;
    private Vector3 newPos;
    // Use this for initialization
    void Start () {
        mouseDelta = Vector2.zero;
        startPos = transform.localPosition;
	}
	
	// Update is called once per frame
	void Update () {
        newPos = transform.position;
        CalculateMouseDelta();
        weaponDelta.x = mouseDelta.x * Time.deltaTime * swayAmount;
        weaponDelta.y = mouseDelta.y * Time.deltaTime * swayAmount;
        weaponDelta.z = 0;
        newPos = startPos + weaponDelta;

        transform.localPosition = Vector3.Lerp(transform.localPosition, newPos, Time.deltaTime * swaySpeed);
       
    }

    void CalculateMouseDelta()
    {
        mouseDelta.x = Input.GetAxis("Mouse X");
        mouseDelta.y = Input.GetAxis("Mouse Y");
    }
}