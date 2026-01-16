using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlayerAreaDetector : MonoBehaviour
{
    [SerializeField] private int playersNeeded = 2;

    private List<GameObject> playersInArea = new List<GameObject>();

    private void OnTriggerEnter(Collider other)
    {
    if (other.CompareTag("Player"))
    {
        // Check if the player is already in the list
        if (!playersInArea.Contains(other.gameObject))
        {
            // Add the player to the list of players in the area
            playersInArea.Add(other.gameObject);

            // Check if the required number of players are in the area
            if (playersInArea.Count >= playersNeeded)
            {
                Debug.Log("All players are in the area!");
            }
        }
    }
    }

    private void OnTriggerExit(Collider other)
    {
        if (other.CompareTag("Player"))
        {
            // Remove the player from the list of players in the area
            playersInArea.Remove(other.gameObject);

            // Check if the required number of players are no longer in the area
            if (playersInArea.Count < playersNeeded)
            {
                Debug.Log("Not enough players in the area!");
            }
        }
    }
}