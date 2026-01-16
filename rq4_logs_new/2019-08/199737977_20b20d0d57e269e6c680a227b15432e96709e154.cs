using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class MouseTrapScript : MonoBehaviour
{
    // Public variables
    public Mesh trapReadyModel;
    public Mesh trapSprungModel;

    private void OnCollisionEnter(Collision collision)
    {
        if (collision.collider.tag == "Rat")
        {
            // Collide with rat
            // Rat dead

            TriggerTrap();
        }
        if (collision.collider.tag == "Player")
        {
            // Collider with player
            collision.collider.GetComponent<PlayerScript>().Dead();
            TriggerTrap();
        }
    }

    private void TriggerTrap()
    {
        this.GetComponent<MeshFilter>().mesh = trapSprungModel;
    }
}