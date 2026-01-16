using UnityEngine;
using System.Collections;

public class newmovement : MonoBehaviour {
    private Rigidbody snowman;
    public float rotateRate = 90f;
    public float walkRate = 35f;

    Quaternion holdsNextRotation; // this is the place you want to turn to
    private float turnInput;
    private float walkInput;


	void Start () {
        snowman = GetComponent<Rigidbody>();
        holdsNextRotation = transform.rotation; //gets current spawn coordinates
        walkInput = turnInput = 0; //setting all the current inputs to 0
     
	}

    void GetInput()
    {
        walkInput = Input.GetAxis("Vertical"); //getAxis returns -1 to 1 
        turnInput = Input.GetAxis("Horizontal"); 

    }
	
	void Update () {
        GetInput(); //gathers your input in update
        Turn(); //turning does not require physics
        
    }

    void FixedUpdate()
    {
        Walk();
    }

    void Walk()
    {
        snowman.velocity = transform.forward * walkInput * walkRate;
        //transform.forward = moving on the snowman's forward axis
        //walkInput is pos/neg which determines forward/backward movement
        // walkRate multiplies the rate at which u want to go 
        print("player is moving");
    }

    void Turn()
    {
        //Angle Axis takes angle from 0-360 that you want to turn and axis
        holdsNextRotation *= Quaternion.AngleAxis(rotateRate*turnInput*Time.deltaTime,Vector3.up); //vector3.up is global yaxis
        // multiplying Quarternion bc its like ur adding angles
        transform.rotation = holdsNextRotation;

    }
}