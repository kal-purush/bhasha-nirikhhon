using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Grab : MonoBehaviour {
    Rigidbody body;
    bool held = false;
    Vector3 temp;
    float distance;

    public GameObject grabber;
    public float throwForce = 600;
    public float minDistance = 1f;

    void Start() {
        body = this.GetComponent<Rigidbody>();
    }
    void Update() {
        distance = Vector3.Distance(this.transform.position, grabber.transform.position);
        if (distance >= minDistance) {
            held = false;
        }
        if (held == true) {
            body.velocity = Vector3.zero;
            body.angularVelocity = Vector3.zero;
            this.transform.SetParent(grabber.transform);
            if (Input.GetMouseButtonDown(1)) {
                held = false;
                body.AddForce(grabber.transform.forward * throwForce);
            }
        } else {
            temp = this.transform.position;
            this.transform.SetParent(null);
            body.useGravity = true;
            this.transform.position = temp;
        }
    }
    void OnMouseDown() {
        if (distance <= minDistance) {
            held = true;
            body.useGravity = false;
            body.detectCollisions = true;
            var grabberAngles = grabber.transform.rotation.eulerAngles;
            this.transform.rotation = Quaternion.Euler(grabberAngles.x + 90, grabberAngles.y, grabberAngles.z);
        }
    }
    void OnMouseUp() {
        held = false;
    }
}