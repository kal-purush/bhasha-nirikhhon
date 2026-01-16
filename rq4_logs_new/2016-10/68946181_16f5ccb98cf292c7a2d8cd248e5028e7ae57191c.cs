using UnityEngine;
using System.Collections;
using System;

public class Character : MonoBehaviour {

    
    private Rigidbody2D rbody;
    private bool isRunning = false;
    Animator anim;
    Vector2 restedPos;
    //Lock controller during acting (any state other than the player having direct control is acting.)
    private bool isActing = false;
    private bool isBlocking = false;
    private int currentHealth;


    public int maxHealth = 10;    
    public float walkspeed = 4;
    public float runspeed = 8;

   
    
    void Start () {
        rbody = GetComponentInChildren<Rigidbody2D>();
        currentHealth = maxHealth;
        anim = GetComponent<Animator>();
    }

    /// <summary>
    /// Handles movement.
    /// </summary>
    /// <param name="movement_vector"></param>
    public void Move(Vector2 movement_vector)
    {



        if (movement_vector != Vector2.zero)
        {
            anim.SetBool("IsWalking", true);
            anim.SetFloat("Input_X", movement_vector.x);
            anim.SetFloat("Input_Y", movement_vector.y);
        }
        else {
            anim.SetBool("IsWalking", false);
        }

        if (movement_vector != Vector2.zero && isActing != true)
        {
            
            if (isRunning)
            {
                rbody.MovePosition(rbody.position + movement_vector * Time.deltaTime * runspeed);
            }
            else
            {
                rbody.MovePosition(rbody.position + movement_vector * Time.deltaTime * walkspeed);
            }
        }

    }

    /// <summary>
    /// Toggles whether or not the player is walking or running.
    /// </summary>
    public void ToggleSpeed()
    {
        isRunning = !isRunning;
    }
    
    /// <summary>
    /// When the character is hit.
    /// </summary>
    public bool onHit(int damage)
    {
        if (isBlocking)
        {
            return false;
        }
        
        currentHealth -= damage;
        isDead();
        
        return true;
    }

    public bool ActionOne()
    {
        //Implement based on equiped item.

        return true;
    }

    public bool ActionTwo()
    {
        //Implement based on equiped item.

        return true;
    }

    public void Dodge()
    {
        //Implement based on current movement direction, and somehow make it over time.
    }



    /// <summary>
    /// Checks if the player's health is currently 0.
    /// </summary>
    /// <returns></returns>
    public bool isDead()
    {
        if(currentHealth <= 0)
        {
            isActing = true;
            return true;
        }
        else
        {
            return false;
        }

    }

    void OnTriggerEnter(Collider other)
    {
        if (other.gameObject.CompareTag("Bonfire"))
        {
            restedPos = new Vector2(transform.position.x, transform.position.y);
        }
    }


}