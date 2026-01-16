using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PowerUps : MonoBehaviour
{
    public GameObject pickupEffect;

    public float multiplier = 1.5f;

    void OnTriggerEnter (Collider other)
    {
        if (other.CompareTag("Player"))
        {
            Pickup(other);
        }
    }

    void Pickup(Collider player)
    {
        Debug.Log("Power Up Picked Up");
        // spawn effect
        //Instantiate(pickupEffect, transform.position, transform.rotation);

        //apply effect
        player.transform.localScale *= multiplier;

        //remove object
        Destroy(gameObject);
    }
}