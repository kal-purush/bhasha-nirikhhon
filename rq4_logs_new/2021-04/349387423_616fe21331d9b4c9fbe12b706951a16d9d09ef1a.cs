using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class SimpleEnemyAi : MonoBehaviour
{

    public bool reachedDestination;
    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        if(transform.position != destination){
            Vector3 destinationDirection = destination - transform.position;
            destinationDirection.y = 0; 

            float destinationDistance = destinationDirection.magnitude;

            if(destinationDistance >= stopDistance){
                reachedDestination = false;
                Quaterion targetRotation = 
            }
        }
    }

    public void SetDestination(Vector3 destination){
        this.destination = destination;
        reachedDestination = false;
    }
}