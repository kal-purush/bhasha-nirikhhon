using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Duncan : MonoBehaviour
{
    public float speed = 4;
    // Start is called before the first frame update
    void Start()
    {
    }

    // Update is called once per frame
    void Update()
    {
    }
    void OnCollisionEnter2D(Collision2D collision)
    {
        ContactPoint2D contact = collision.contacts[0];
        if(contact.collider.gameObject.tag == "Wall"){
            Destroy(gameObject);
        }
    }
}