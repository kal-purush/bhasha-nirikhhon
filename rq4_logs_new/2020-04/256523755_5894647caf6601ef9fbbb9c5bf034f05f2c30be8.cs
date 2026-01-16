using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class playerHealth : MonoBehaviour
{
    public int healthPoints = 10;
    public string whoCollide;
    GameObject player;
    GameObject enemy;
    Animator anim;

    void Start()
    {
        player = GameObject.FindWithTag("Player");
        enemy = GameObject.FindWithTag("Enemy");
        anim = GetComponent<Animator>();
    }


    void OnTriggerEnter (Collider col)
    {
        whoCollide = col.gameObject.tag;
        //print(whoCollide + col.gameObject.GetComponent<weaponCollider>().damage); //check if the attack deals dmg according to weapon power
        healthPoints -= col.gameObject.GetComponent<weaponCollider>().damage;
        print("remaining health: " + healthPoints);
        anim.SetTrigger("gotHit"); //ANIMATION
        
    }
    
    void PlayerDead()
    {
        print("PLAYER DEAD!!!!");
        healthPoints = 10;
    }
    void Update()
    {
        //if(whoCollide == "enemyWeapon")

        if (healthPoints == 0)
        {
            PlayerDead();

        }
    }
}