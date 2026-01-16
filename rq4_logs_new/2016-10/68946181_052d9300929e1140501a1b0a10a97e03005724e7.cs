using UnityEngine;
using System.Collections;
using System.Collections.Generic;

public class HeartManager : MonoBehaviour {

    public List<GameObject> hearts;
    int healthPerHeart = 2;

    // This will probably be handled by the health script. Need to get together with whoever is programming that and work this out.
    int startingHealth = 6;

	void Start ()
    {
        int healthToAdd = startingHealth;

        // Turn on the correct number of hearts at the beggining of the game
	    foreach (GameObject heart in hearts)
        {
            if (healthToAdd > 0)
            {
                heart.GetComponent<UIHearts>().DisplayFullHeart();
                healthToAdd -= healthPerHeart;
            }    
        }
	}
	
	
}