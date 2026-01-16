using UnityEngine;
using System.Collections;

public class PlayerController : MonoBehaviour 
{
    public Rigidbody player;

    //The Players MoveSpeed
    public float moveSpeed = 10;
    //The Strength of the players Jump
    public float jumpStrength = 50;
    //Chi Level of Player (max 3)
    public int chi = 3;
    //Informs the engine that the player is grounded.
    public bool isGrounded = false;
    //Players Health
    public int playersHealth = 100;

	// Use this for initialization
	void Start () 
    {
        player = GetComponent<Rigidbody>();
	}
	
	// Update is called once per frame
	void Update () 
    {
        
	}

    void OnTriggerEnter(Collider other)
    {
        isGrounded = true;
    }

    void OnTriggerExit(Collider other)
    {
        isGrounded = false;
    }
    void FixedUpdate()
    {
        /////Player Movement/////
        //Moves the player Left
        if(Input.GetKey(KeyCode.A))
        {
            player.AddForce(-transform.right * moveSpeed);
        }
        //Moves the player right
        if (Input.GetKey(KeyCode.D))
        {
            player.AddForce(transform.right * moveSpeed);
        }
        //Jump
        if(Input.GetKeyDown(KeyCode.Space) && chi > 0)
        {
            player.AddForce(transform.up * jumpStrength);
            chi -= 1;
        }
        if(isGrounded == true)
        {
            chi = 2;
        }
    }
}