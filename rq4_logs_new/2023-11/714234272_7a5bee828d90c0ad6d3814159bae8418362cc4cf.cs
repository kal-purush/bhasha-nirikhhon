using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlayerAnimation : MonoBehaviour
{
    Rigidbody body;
    Animator animator;
    
    // Start is called before the first frame update
    void Start()
    {
        body = GetComponentInParent<Rigidbody>();
        
        animator = GetComponent<Animator>();
        
    }

    // Update is called once per frame
    void Update()
    {
        if (Mathf.Abs(body.velocity.x) > 1 || Mathf.Abs(body.velocity.z) > 1)
        {
            animator.SetBool("isRunning", true);
        } else
        {
            animator.SetBool("isRunning", false);
        }
    }
}