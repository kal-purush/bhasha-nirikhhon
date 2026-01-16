using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlayerController : MonoBehaviour
{
    public float maxSpeed;

    private Rigidbody2D rb2d;

    // Start is called before the first frame update
    void Start()
    {
        rb2d = GetComponent<Rigidbody2D>();
    }

    // Update is called once per frame
    void Update()
    {

    }

    void FixedUpdate()
    {
        float hSpeed = Input.GetAxis("Horizontal") * maxSpeed;
        float vSpeed = Input.GetAxis("Vertical") * maxSpeed;
        rb2d.velocity = new Vector2(hSpeed, vSpeed);
    }
}