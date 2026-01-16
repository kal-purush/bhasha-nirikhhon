using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class EnemyAttackManager : MonoBehaviour
{
    public int damage;
    
    public float interval;
    public float timer;

    public bool attack = false;


    public Transform target;
    public GameObject flameParticle;

    private void Update()
    {
        timer += Time.deltaTime;
        if (timer >= interval)
        {
            timer = 0;
        }
    }

    private void OnTriggerEnter2D(Collider2D collision)
    {
        if (collision.CompareTag("Player"))
        {
           if (timer == 0)
            {
                attack = true;
                print("hurt");
                collision.GetComponent<PlayerHealthManager>().hurtPlayer(damage);
                Instantiate(flameParticle, target.transform.position, Quaternion.identity);
            }

        }
        else
        {
            attack = false;
        }
    }
    
}