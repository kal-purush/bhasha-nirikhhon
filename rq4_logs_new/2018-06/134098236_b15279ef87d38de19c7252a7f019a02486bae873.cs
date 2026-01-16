using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Interact : MonoBehaviour {

    public GameObject pickUp = null;
    bool pickedUpRock = false;
    bool pickedUpStick = false;

    void Update()
    {
        //do stuff if E key is pressed
        if (Input.GetKeyDown(KeyCode.E) && pickUp)
        { 
            if (pickUp.CompareTag("stick"))
            {
                pickedUpStick = !pickedUpStick;
                if (pickedUpStick == false) pickUp = null;
            }
            else if (pickUp.CompareTag("rock"))
            {
                
                pickedUpRock = !pickedUpRock;
                if (pickedUpRock == false) pickUp = null;
            }
           
        }
       
        //pickup changes to position relative to player
        if ((pickedUpRock || pickedUpStick) && pickUp) { pickUp.transform.position = new Vector2(gameObject.transform.position.x + 1, gameObject.transform.position.y + 1); }

    }

    //triggers when player enters in and out of range of items
    void OnTriggerEnter2D(Collider2D other)
    {
        
        if (other.CompareTag("stick") && pickedUpRock == false)
        {
            Debug.Log(other.name);
            pickUp = other.gameObject;
        }

        if (other.CompareTag("rock") && pickedUpStick == false)
        {
            Debug.Log(other.name);
            pickUp = other.gameObject;
        }
    }

    void OnTriggerExit2D(Collider2D other)
    {
        if ((other.gameObject == pickUp) && !(pickedUpRock || pickedUpStick))
        {
            pickUp = null;
        }
    }
}