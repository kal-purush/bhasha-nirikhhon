using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class helperPlayer : MonoBehaviour
{
    public float speed;
    public Rigidbody2D rb;
    // Start is called before the first frame update
    void Start()
    {
        rb = GetComponent<Rigidbody2D>();
    }

    // Update is called once per frame
    void Update()
    {
        float horizontal = Input.GetAxisRaw("Horizontal");
        rb.velocity = new Vector3(horizontal * speed, rb.velocity.y, 0);
    }
}