using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class EnemyBehavior : MonoBehaviour {

    public Transform Player;

    // Enemy Info
    public int MoveSpeed = 5;
    public int AttackRange = 2;
    public int AttackDamage = 1;
    private int Health = 2;

	// Use this for initialization
	void Start () {
		
	}
	
	// Update is called once per frame
	void Update () {
		
        // Make sure that player exists
        if(Player != null)
        {
            // Always look at Player
            transform.LookAt(Player);

            /****
            ** If enemy is close by, then attack
            */
            if (Vector3.Distance(transform.position, Player.position) <= AttackRange)
            {

            } else
            // If enemy is not close, move towards player
            {
                // Assumes always looking at player and moves towards them
                transform.position += transform.forward * MoveSpeed * Time.deltaTime;
            }
            /*
            ** 
            ****/

        }

        
	}

    // Function for taking damage
    void TakeDamage(int damage)
    {
        this.Health -= damage;
    }

    // Function for dying
    void Death()
    {

    }

    // Function for exploding
    void Explode()
    {

    }
}