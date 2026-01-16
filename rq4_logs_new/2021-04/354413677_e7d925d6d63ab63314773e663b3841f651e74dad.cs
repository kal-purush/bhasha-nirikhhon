using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class DamageArea : MonoBehaviour
{

    public int damageOverTime = 1;

    private void OnTriggerEnter2D(Collider2D other)
    {
        if(other.CompareTag("Player"))
        {
            //Debug.Log("player entered in damage zone");
        }
    }
    private void OnTriggerStay2D(Collider2D other)
    {
        if(other.CompareTag("Player"))
        {
            //Debug.Log("player take damage");
            other.gameObject.GetComponentInChildren<Player>().DoDamage();
        }
    }
    private void OnTriggerExit2D(Collider2D other)
    {
        if(other.CompareTag("Player"))
        {
            //Debug.Log("player exited from damage zone");
        }
    }
}