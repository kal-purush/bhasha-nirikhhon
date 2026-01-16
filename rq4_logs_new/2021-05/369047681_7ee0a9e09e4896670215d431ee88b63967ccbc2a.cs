using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class DamageObjetct : MonoBehaviour
{
    public void OnCollisionEnter2D(Collision2D collision)
    {


        if (collision.transform.CompareTag("Player"))
        {
            Debug.Log("Da�o");
            collision.transform.GetComponent<Respawn>().PlayerDamage();
        }
    }
    
}