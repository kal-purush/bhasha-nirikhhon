using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class DestroyOnCollected : MonoBehaviour
{

    /* TLDR
    if the key has been collided with a player, dont spawn it again when we walk back into it
    */
    static bool isShown = true;

    void Start()
    {
        this.gameObject.SetActive(isShown);
    }
    void OnCollisionEnter2D(Collision2D other)
    {
        if (other.gameObject.tag == "Player")
            isShown = false;
    }

    public void ResetAllVals()
    {
        isShown = true;
    }

}