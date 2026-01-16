using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Explode : MonoBehaviour
{
    // Start is called before the first frame update
    void Start()
    {
        Invoke("remove", 0.2f);
    }

    // Update is called once per frame
    void Update()
    {
        
    }

    void remove()
    {
        Destroy(this.gameObject);
    }
    /*
    private void OnTriggerEnter2D(Collider2D collision)
    {
        if (collision.gameObject.tag == "Player")
        {
            var force = transform.position - collision.transform.position;
            force.Normalize();
            GetComponent<Rigidbody2D>().AddForce(-force * 500);
        }
    }
    */
}