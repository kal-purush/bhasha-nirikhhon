using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class movement : MonoBehaviour
{
    public int speed;
    private float inputHorizontal;
    private Rigidbody rb;
    private bool jumping = true;

    void Start() {
        rb = GetComponent<Rigidbody>();
    }

    // Update is called once per frame
    void Update() {
        inputHorizontal = Input.GetAxis("Horizontal");

        Vector2 direction = new(inputHorizontal, 0);
        transform.Translate(direction*Time.deltaTime*speed);

        if (Input.GetButtonDown("Jump") && !jumping) {
            rb.AddForce(new Vector2(0, speed*1.5f), ForceMode.Impulse);
            jumping = true;
        }
    }

    private void OnCollisionEnter(Collision collision) {
        if (collision.gameObject.GetComponent<Rigidbody>() || collision.gameObject.name == "Plane") {
            jumping = false;
        }
    }
}