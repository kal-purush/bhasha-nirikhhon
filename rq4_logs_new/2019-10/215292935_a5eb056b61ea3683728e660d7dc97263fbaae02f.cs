using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class DestroySimpleBoullets : MonoBehaviour
{
    private float xOutOfBound = 15.0f;
    private float zOutOfBound = 7.4f;

    // Start is called before the first frame update
    void Start()
    {

    }

    // Update is called once per frame
    void Update()
    {
        if (transform.position.x < -xOutOfBound || transform.position.z > xOutOfBound ||
            transform.position.z < -3 * zOutOfBound || transform.position.z > zOutOfBound) 
        {
            Destroy(gameObject);
        }
    }
}