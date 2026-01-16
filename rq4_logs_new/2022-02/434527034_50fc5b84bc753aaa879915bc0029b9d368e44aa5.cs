using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class BasicEnAttack : Ability
{
    
    public float damage;
    public float attackRange;
    private LayerMask playerLayer;

    private void Awake()
    {
        remainingCooldown = 0;
        playerLayer = LayerMask.GetMask("Player");
    }

    private void Update()
    {
        if(remainingCooldown > 0)
        {
            remainingCooldown -= Time.deltaTime;
        }
    }


    public override void Cast()
    {
        if(remainingCooldown <= 0)
        {
            Collider2D hit = Physics2D.OverlapCircle(castPoint.position, attackRange, playerLayer);

            if(hit != null)
            {
                hit.gameObject.GetComponent<PlayerStats>().TakeDamage(damage);
            }

            remainingCooldown = cooldown;

            Debug.Log("Attacking");
        }
    }

    public override void LevelUp(int levelToGetTo)
    {
        while(level < levelToGetTo)
        {
            damage = 1.15f*damage;
            level++;
        }
    }

    private void OnDrawGizmosSelected()  //For debugging purposes
    {
        Gizmos.color = Color.red;
        Gizmos.DrawWireSphere(castPoint.position, attackRange);
    }
}