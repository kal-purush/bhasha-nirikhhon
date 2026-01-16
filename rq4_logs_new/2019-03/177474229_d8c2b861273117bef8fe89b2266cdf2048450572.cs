using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlayerPlatformerController : MonoBehaviour
{


    public float speed = 1f;
 
    protected Rigidbody rigidbody;
 
    Vector3 move;

 

    void OnEnable()
    {
        rigidbody = GetComponent<Rigidbody>();
    }

    void Start()
    {
         move = transform.position;

    }

    void Update()
    {
    }

    protected virtual void ComputeVelocity()
    {

    }

    void FixedUpdate()
    {


        move.x += Input.GetAxis("Horizontal") * speed;
        move.y += Input.GetAxis("Vertical") * speed;
        
        rigidbody.MovePosition(move);

 
    }
    

    public float maxSpeed = 7;
    public float jumpTakeOffSpeed = 7;
    
    
}