using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Brain : MonoBehaviour
{private float x;
    private float z;
    private bool rotateX;
    private float rotationSpeed;

    
 //   GameObject power;

    void Start()
    {
        x = 0.0f;
        z = 0.0f;
        rotateX = true;
        rotationSpeed = 75.0f;
    }

    void FixedUpdate()
    {
        
            x += Time.deltaTime * rotationSpeed;

        

        transform.localRotation = Quaternion.Euler(0, x, 0);
    }
    private void OnTriggerEnter(Collider other) {
        if(other.name == "aj"){
          //  recoger();
        }
    }
  
}