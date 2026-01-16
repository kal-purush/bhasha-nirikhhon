using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlayerController : MonoBehaviour
{
    private bool isGrounded = false;
    
    // Gravity Scale editable on the inspector
    // providing a gravity scale per object
 
    public float gravityScale = 1.0f;
 
    // Global Gravity doesn't appear in the inspector. Modify it here in the code
    // (or via scripting) to define a different default gravity for all objects.
 
    public static float globalGravity = -9.81f;
 
    Rigidbody m_rb;
    
    // Start is called before the first frame update
    void Start()
    {
        Debug.Log(m_rb.velocity.y);
    }

    // Update is called once per frame
    void Update()
    {
        if (!isGrounded && Input.GetKey(KeyCode.Space))
        {
            gravityScale = 0.00001f;
            Debug.Log(m_rb.velocity.y);
            float v = Input.GetAxisRaw("Vertical");
            float h = Input.GetAxisRaw("Horizontal");
            this.transform.Translate(h * 10, 0, v * 10);
        }
        else if (!isGrounded)
        {
            gravityScale = 1f;
        }
    }

    void OnEnable ()
    {
        m_rb = GetComponent<Rigidbody>();
        m_rb.useGravity = false;
    }
 
    void FixedUpdate ()
    {
        Vector3 gravity = globalGravity * gravityScale * Vector3.up;
        m_rb.AddForce(gravity, ForceMode.Acceleration);
    }
}