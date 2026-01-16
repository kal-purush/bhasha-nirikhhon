using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Speedboost : MonoBehaviour
{   //script doesnt work yet
    // Start is called before the first frame update
    public GameObject powerUpBoxEffect;

    void OnTriggerEnter(Collider other)
     {
        Debug.Log("Collision");
        if(other.CompareTag("Player"))
            {
             boostSpeed();
             Debug.Log("something");
            }
     }

     void boostSpeed()
     {
      Debug.Log("Speed Boost Aquired");

      Instantiate(powerUpBoxEffect, transform.position, transform.rotation);
      //FindObjectOfType<CarController>().speed ;
      Destroy(gameObject);
	 }

}