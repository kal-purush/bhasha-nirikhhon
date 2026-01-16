using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class FishMovement : MonoBehaviour
{
    private Vector3 pos;
    [Range(-5,5)]
    public float speed = 1;

    // Use this for initialization
    void Start()
    {
        // starting
        pos = gameObject.transform.position;
    }

    // Update is called once per frame
    void Update()
    {
        //Mathf.Sin
        pos += Vector3.back * Time.deltaTime * speed;
        gameObject.transform.position = pos;
   
    }

    void OnCollisionEnter(Collision collision)
    {
    }
}