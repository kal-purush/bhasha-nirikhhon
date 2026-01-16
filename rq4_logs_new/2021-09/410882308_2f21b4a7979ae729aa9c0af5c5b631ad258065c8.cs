using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class BulletScript : MonoBehaviour
{
    public BulletScriptable stat;
    private Rigidbody2D rb;

    private void Start()
    {
        rb = GetComponent<Rigidbody2D>();
        rb.AddForce(Vector2.down * stat.bulletSpeed);
    }

    void Update()
    {
        
    }

    private void OnTriggerEnter2D(Collider2D other)
    {
        Debug.Log("enter Collider");
         
       if (other.tag == "Player")
        {
         other.GetComponent<PlayerController>().life--; 
         gameObject.SetActive(false);
         Debug.Log("touch player");
        }
        else
        {
            Debug.Log("touch Enemy");
            other.GetComponent<EnemyBehaviour>().life--;
            gameObject.SetActive(false);
            
        }
        
    }
}