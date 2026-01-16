using System.Collections.Generic;
using UnityEngine;

public class SS_InteractiveController : MonoBehaviour
{
    // private List<GameObject> purchaseObjects;

    // private void OnTriggerEnter(Collider other)
    // {
    //     // Check if the other has the tag of item and also check if there is a gameobject with the tag of Coin contained in the purchaseObjects list.
    //     if (other.gameObject.tag != "Item") return;

    //     // Add the other gameobject to the purchaseObjects list.
    //     purchaseObjects.Add(other.gameObject);

    //     // Begin the purchase process.
    //     GameManager.instance.OnPurchaseBegin();

    //     // Check if the purchaseObjects list contains a gameobject with the tag of Coin using a for loop.
    //     for (int i = 0; i < purchaseObjects.Count; i++)
    //     {
    //         Debug.Log(purchaseObjects[i].tag);

    //         if (purchaseObjects[i].tag == "Coin")
    //         {
    //             // If the purchaseObjects list contains a gameobject with the tag of Coin, then call the OnPurchase event.
    //             GameManager.instance.OnPurchaseCompleted();
    //         }
    //     }
    // }

    private List<GameObject> coins = new List<GameObject>();
    private List<GameObject> purchaseObjects = new List<GameObject>();

    private void OnTriggerEnter(Collider other)
    {
        if (other.gameObject.tag == "Item")
        {
            if (purchaseObjects.Count == 0)
                GameManager.instance.OnPurchaseBegin();

            purchaseObjects.Add(other.gameObject);
        }
        else if (other.gameObject.tag == "Coin")
        {
            coins.Add(other.gameObject);
        }

        if (other.gameObject.tag == "Coin" && purchaseObjects.Count > 0)
            GameManager.instance.OnPurchaseCompleted();
    }

    private void OnTriggerExit(Collider other)
    {
        if (other.gameObject.tag == "Item")
        {
            purchaseObjects.Remove(other.gameObject);

            if (purchaseObjects.Count == 0)
                GameManager.instance.OnPurchaseAborted();
        }
        else if (other.gameObject.tag == "Coin")
        {
            coins.Remove(other.gameObject);
        }
    }
}