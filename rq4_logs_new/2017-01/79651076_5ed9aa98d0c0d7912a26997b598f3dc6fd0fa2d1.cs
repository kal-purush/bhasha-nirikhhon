using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class MouseFollowInput : MonoBehaviour {
    public Transform pivot;
    Vector3 velocity;

    void Start() {
        if (!pivot)
            Debug.LogError("MouseFollowInput.pivot not set!");
    }

    void Update() {
        Vector3 myWorldPos = pivot.position;
        Camera cam = Camera.main;
        Vector3 camWorldPos = cam.transform.position;

        Vector3 mouseScreenPos = Input.mousePosition;
        mouseScreenPos.z = (myWorldPos - camWorldPos).magnitude;

        Vector3 mouseWorldPos = cam.ScreenToWorldPoint(mouseScreenPos);
        pivot.LookAt(mouseWorldPos, Vector3.up);

        if (mouseWorldPos.sqrMagnitude < 1) {
            velocity = Vector3.zero;
        } else {
            velocity += Time.deltaTime * 0.5f * pivot.forward;
        }

        Vector3 pos = pivot.transform.position;
        pos += velocity;
        pos.y = 0;
        pivot.transform.position = pos;
    }
}