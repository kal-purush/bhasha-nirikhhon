using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.XR.Interaction.Toolkit;

public class AntiSoftBlockProtectionAB : MonoBehaviour
{
    // If the moneys have entered this area, then it means that the player is soft blocked.
    // To prevent this, make the moneys attached to the players socket interactor.

    private void OnTriggerEnter(Collider other)
    {
        if (other.gameObject.tag == "Coin")
        {
            print("You are soft blocked!");
            
            // Get the player's socket interactor.
            XRSocketInteractor socketInteractor = GameObject.FindGameObjectWithTag("CoinPurse").GetComponent<XRSocketInteractor>();

            // Set the interactor's attach transform to the player's socket interactor's attach transform.
            
        }
    }

}