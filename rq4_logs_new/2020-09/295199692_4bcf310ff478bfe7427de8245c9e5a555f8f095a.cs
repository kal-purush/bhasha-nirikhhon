using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class SwitchGravity : MonoBehaviour
{
    public GameObject grounchCheck;

    private Rigidbody2D rb;
    private bool switchedGravity;

    // Start is called before the first frame update
    void Start()
    {
        rb = GetComponent<Rigidbody2D>();
    }

    void Update()
    {
        switchedGravity = Input.GetKeyDown(KeyCode.Q);
    }

    void FixedUpdate()
    {
        if (switchedGravity)
        {
            rb.gravityScale = rb.gravityScale * -1;
        }
    }
}