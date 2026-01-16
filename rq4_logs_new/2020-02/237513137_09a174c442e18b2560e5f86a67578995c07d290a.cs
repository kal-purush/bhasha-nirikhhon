using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class GroundMovement : MonoBehaviour
{
    //private BalloonMove Balloon;
    bool InBalloon = true;

    [SerializeField] private float MoveSpeed = 15.0f;
    [SerializeField] private float JumpVel = 10.0f;
    [SerializeField]
    private Rigidbody rb;
    [SerializeField]
    private bool grounded;

    Vector3 movement;
    // Start is called before the first frame update
    void Start()
    {
        //Balloon = GameObject.Find("Balloon").GetComponent<BalloonMove>();  
    }

    // Update is called once per frame
    public void Update()
    {
        CalculateMovement();
    }

    public void CalculateMovement()
    {
        float x = Input.GetAxis("Horizontal");
        float z = Input.GetAxis("Vertical");

        movement = transform.right * x + transform.forward * z;

        if (Input.GetKeyDown(KeyCode.Space) && grounded)
        {
            rb.velocity = Vector3.up * JumpVel;
            grounded = false;
        }

    }

    private void FixedUpdate()
    {
        rb.MovePosition(rb.position + (movement * MoveSpeed * Time.deltaTime));
    }

    private void OnCollisionEnter(Collision Collider)
    {
        //TODO Ground collision

        if (Collider.transform.position.y < transform.position.y)
        {
            grounded = true;
        }
    }

    void OnTriggerEnter(Collider other)
    {
        if (other.tag == "Owner1" && Input.GetKey(KeyCode.A))
        {
            //NPC 1 dialogue. Will add soon
        }

        if (other.tag == "Owner2" && Input.GetKey(KeyCode.A))
        {
            //NPC 2 dialogue. Will add soon
        }

        if (other.tag == "Owner3" && Input.GetKey(KeyCode.A))
        {
            //NPC 3 dialogue. Will add soon
        }

    }
}