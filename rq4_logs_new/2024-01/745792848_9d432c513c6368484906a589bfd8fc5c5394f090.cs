using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class enemy_Attack : MonoBehaviour
{
    public float speed = 3f;
    public float attackDamage = 10f; 
    public float attackSpeed = 1f;
    private float canAttack;
    private Transform target;

    private void Update()
    {
        if(target != null)
        {
            float step = speed * Time.deltaTime;
            transform.position = Vector2.MoveTowards(transform.position, target.position, step);
        }
    }

    private void OnCollisionStay2D(Collision2D other)
    {
        if(other.gameObject.tag == "Player")
        {
            if(attackSpeed <= canAttack){
            other.gameObject.GetComponent<playerHealth>().UpdateHealth(-attackDamage);
            canAttack = 0f;
            }
            else
            {
                canAttack += Time.deltaTime;
            }
        }
    }
    private void OnTriggerEnter2D(Collider2D other)
    {
        if(other.gameObject.tag == "Player")
        {
            target = other.transform;
        }
    }

    private void OnTriggerExit2D(Collider2D other)
    {
        if(other.gameObject.tag == "Player")
        {
            target = null;
        }
    }


}