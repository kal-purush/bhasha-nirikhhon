using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Axis : MonoBehaviour
{
    public float theta;
    public float minTheta;
    public float maxTheta;

    float xAngle;
    float yAngle;
    public float offset;


    private void Start() {
        xAngle = transform.localEulerAngles.x;
        yAngle = transform.localEulerAngles.y;
    }

    private void Update() {
        transform.localEulerAngles = new Vector3(xAngle, yAngle, theta + offset);
    }
}