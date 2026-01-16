using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Unity.VisualScripting;
using UnityEditor.Experimental.GraphView;
using UnityEngine;
using UnityEngine.InputSystem.Android;

public class mineScript : MonoBehaviour
{
    public float fieldofImpact;
    public float force;

    public LayerMask LayerToHit;
    public GameObject ExplosionEffect;

    // Update is called once per frame


    private void OnTriggerEnter2D(Collider2D collider)
    {
        Debug.Log("Triggered!");
        explode();
    }

    void explode()
    {
        Collider2D[] objects = Physics2D.OverlapCircleAll(transform.position, fieldofImpact, LayerToHit);
       

        foreach (Collider2D obj in objects)
        {
            Vector2 direction = obj.transform.position - transform.position;
            
            obj.GetComponent<Rigidbody2D>().AddForce(direction * force);
        }

        GameObject ExplosionEffectIns = Instantiate(ExplosionEffect, transform.position, Quaternion.identity);
        Destroy(ExplosionEffectIns,10);
    }

    private void OnDrawGizmosSelected()
    {
        Gizmos.color = Color.red;
        Gizmos.DrawWireSphere(transform.position,fieldofImpact);
    }
}
    
