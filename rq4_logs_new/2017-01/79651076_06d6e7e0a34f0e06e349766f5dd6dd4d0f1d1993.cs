using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Bounce : MonoBehaviour {
    void OnCollisionEnter(Collision c) {
        var r = c.gameObject.GetComponent<Rigidbody>();
        if (r == null) {
          r = c.gameObject.GetComponentInParent<Rigidbody>();
        }
        if (r == null) {
          r = c.gameObject.GetComponentInChildren<Rigidbody>();
        }
        if (r != null) {
          r.AddForceAtPosition(-c.contacts[0].normal, c.contacts[0].point, ForceMode.VelocityChange);
        }
    }
}