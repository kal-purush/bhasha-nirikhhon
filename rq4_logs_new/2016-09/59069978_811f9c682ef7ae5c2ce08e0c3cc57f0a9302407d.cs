using UnityEngine;
using System;
using System.Collections;

public class BasePlayer : MonoBehaviour {

    // Variables 
    private bool gotHit;
    protected Animator animator;
    protected Rigidbody2D myRig;
    protected GameObject ninjaObj;
    protected float xMovement;
    protected float yMovement;

    //Stats
    private string ninjaName;
    private string ninjaDescription;
    private int experincePoints;
    private int maxHealthPoints;
    private int currentHP;
    private int attackPoints;
    private int defensePoints;
    private int walkSpeed;

    

    public string NinjaName
    {
        get { return ninjaName; }
        set { ninjaName = value; }
    }

    public string NinjaDescription
    {
        get { return ninjaDescription; }
        set { ninjaDescription = value; }
    }

    public int ExperiencePoints
    {
        get { return experincePoints; }
        set { experincePoints = value; }
    }

    public int MaxHealthPoints
    {
        get { return maxHealthPoints; }
        set { maxHealthPoints = value; }
    }

    public int CurrentHP
    {
        get { return currentHP; }
        set { currentHP = value; }
    }

    public int AttackPoints
    {
        get { return attackPoints; }
        set { attackPoints = value; }
    }

    public int DefensePoints
    {
        get { return defensePoints; }
        set { defensePoints = value; }
    }

    public int WalkSpeed
    {
        get { return walkSpeed; }
        set { walkSpeed = value; }
    }

    

    //Controls
    public void Controls()  
    {

        //Player Controls--------------------------------------->
        xMovement = Input.GetAxis("Horizontal");
        yMovement = Input.GetAxis("Vertical");

        myRig.velocity = new Vector2(0, 0);

        //Move up
      
         if (yMovement > 0.3)
         {
             myRig.velocity = new Vector2(myRig.velocity.x, walkSpeed);
         }

        //Move down						

        if (yMovement < -0.3)
        {
            myRig.velocity = new Vector2(myRig.velocity.x, -walkSpeed);
        }

        //Move left

        if (xMovement < -0.3)
        {
            myRig.velocity = new Vector2(-walkSpeed, myRig.velocity.y);
        }

        //Move right

        if (xMovement > 0.3)
        {
            myRig.velocity = new Vector2(walkSpeed, myRig.velocity.y);
        }


    }


    //Handling player hit detection------------------------------>>

    void OnTriggerEnter2D(Collider2D coll)
    {
        //send flag
        Debug.Log("Hit Player");
        //playerObj.SendMessage("addDamage", 100);

    }
		


}