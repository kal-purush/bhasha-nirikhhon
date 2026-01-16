using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ComportamientoPelota : MonoBehaviour
{
    public void OnCollisionEnter(Collision collision)
    {
        if (collision.collider.name == "Pierde")
        {
            Destroy(this.gameObject);
        }
    }

    /*
    public void OnTriggerEnter(Collider other)
    {
        if (other.collider.name == "Pierde")
        {
            Destroy(this.gameObject);
        }
    }
    */


    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        
    }
}