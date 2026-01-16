using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class Collector : MonoBehaviour
{
    // count for cherries, initialize as 0 at beginning
    private int cherryCount = 0;

    // UI-message of player's current cherry amount 
    [SerializeField] private Text cherryCountText;


    // Player collect items
    private void OnTriggerEnter2D(Collider2D collider2D)
    {
        // Player collect cherry
        if (collider2D.gameObject.CompareTag("Cherry"))
        {
            // clear the collected cherry item from Tilemap and update amount
            Destroy(collider2D.gameObject);
            cherryCount++;
            cherryCountText.text = "Cherries: " + cherryCount;
        }
    }
}