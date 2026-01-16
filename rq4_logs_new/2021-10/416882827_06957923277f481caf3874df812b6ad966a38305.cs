using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.AI;

public class enemyController : MonoBehaviour
{

    private string State = "Idle";
    //for animation
    public NavMeshAgent Enemy;
    private Transform target;
    public float spaceDifference = 5;
    public float attackSpace = 1;
    public float speed = 5; 
    public int health;
    public int maxHealth;
     
    // Start is called before the first frame update
    void Start()
    {
        target = GameObject.FindGameObjectWithTag("Player").transform;
        health = maxHealth;
        Enemy = GetComponent<NavMeshAgent>();
    }

    // Update is called once per frame
    void Update()
    {
         float distance = Vector3.Distance(transform.position, target.position);
         

        //if(CharController.gameOver)
        //{
            //aesthetic choices here
        //}

        if(State == "Idle")
            {
               if(distance < spaceDifference)
                {
                   State = "Chase";
                }
            }
        else if(State == "Chase")
        {
            //animation would go here
            Enemy.SetDestination(target.position);

           if(distance < attackSpace)
            {
              State = "Attack";
            }
            
            else if(State == "Attack")
            {
                  if(distance > attackSpace)
                    {
                        State = "Chase";
                    }
            }

        }
    }

    public void Damaged(int damage)
    {
        State = "Chase";
        health -= damage;

        if (health < 0)
        {
            Death();
        }
    }

    private void Death()
    {
        Debug.Log("DEFEAT HAS HAPPENED <3");
        //GetComponent<//Collider>().enabled = false;
        this.enabled = false;
    }
}