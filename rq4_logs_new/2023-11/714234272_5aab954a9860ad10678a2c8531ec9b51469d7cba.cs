using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.InputSystem;

public class PlayerMovement : MonoBehaviour
{
    Rigidbody body;
    Vector2 moveInput;
    float moveSpeed = 5f;
    bool facingRight = true;
    // Start is called before the first frame update
    void Start()
    {
        body = GetComponent<Rigidbody>();
    }

    // Update is called once per frame
    void Update()
    {
        Run();
    }

    void OnMove(InputValue value)
    {
        moveInput = value.Get<Vector2>();
    }

    void Run()
    {
        body.velocity = new Vector3((moveInput.x * moveSpeed), body.velocity.y, (moveInput.y * moveSpeed));
        
        if (body.velocity.x > 0)
        {
            if (!facingRight)
            {
                gameObject.transform.localScale = new Vector2(1, 1);
                facingRight = true;
            }
            
        } else if (body.velocity.x < 0)
        {
            if (facingRight)
            {
                gameObject.transform.localScale = new Vector2(-1, 1);
                facingRight = false;
            }
            
        }
    }


}