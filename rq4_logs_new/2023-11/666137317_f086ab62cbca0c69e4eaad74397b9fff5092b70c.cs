using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class MudMechanic : MonoBehaviour
{
   /* // Adjust this value to control the slowdown factor
    public float slowdownFactor = 0.5f;

    // List to keep track of objects inside the collider
    private List<Collider> collidersInside = new List<Collider>();

    private void OnTriggerEnter(Collider other)
    {
        // Check if the entering object is the player
        if (other.CompareTag("Player"))
        {
            // Add the player to the list of colliders inside
            collidersInside.Add(other);
            // Slow down the player's movement
            PlayerMovement playerMovement = other.GetComponent<PlayerMovement>();
            if (playerMovement != null)
            {
                playerMovement.SlowPlayer(slowdownFactor);
            }
        }
    }

    private void OnTriggerExit(Collider other)
    {
        // Check if the exiting object is the player
        if (other.CompareTag("Player"))
        {
            // Remove the player from the list of colliders inside
            collidersInside.Remove(other);
            // If no players are inside, reset the slowdown
            if (collidersInside.Count == 0)
            {
                PlayerMovement playerMovement = other.GetComponent<PlayerMovement>();
                if (playerMovement != null)
                {
                    playerMovement.RestorePlayerSpeed();
                }
            }
        }
    }*/
}