using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Powerup : MonoBehaviour
{
    private bool isCollected = false;

    private void OnTriggerEnter2D(Collider2D collision)
    {
        //player 1
        if (collision.CompareTag("Player_01"))
        {
            Hamster1Control ham = collision.GetComponent<Hamster1Control>();
            if (ham != null)
            {
                ham.ActivatePowerup();
            }

            isCollected = true;

            Destroy(gameObject);
        }
        //player 2 
        else if (collision.CompareTag("Player_02"))
        {
            Hamster2Control ham2 = collision.GetComponent<Hamster2Control>();
            if (ham2 != null)
            {
                ham2.ActivatePowerup();
            }
            isCollected = true;

            Destroy(gameObject);
        }
    }
}