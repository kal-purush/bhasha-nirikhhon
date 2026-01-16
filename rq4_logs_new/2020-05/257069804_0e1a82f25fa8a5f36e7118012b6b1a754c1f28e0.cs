using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Tank : MonoBehaviour
{
    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        
    }

    //Detect collisions between the GameObjects with Colliders attached
    // void OnCollisionEnter(Collision collision)
    // {
    //     //Check for a match with the specified name on any GameObject that collides with your GameObject
    //     if (collision.gameObject.name == "Tank_1" || collision.gameObject.name == "Tank_2")
    //     {
    //         Rigidbody targetRigidbody = this.GetComponent<Rigidbody> ();
    //         Debug.Log(targetRigidbody);
    //         TankHealth targetHealth = targetRigidbody.GetComponent<TankHealth> ();

    //         // Deal this damage to the tank.
    //         targetHealth.TakeDamage (100.0f);
    //     }
    // }
}