using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlayerController : MonoBehaviour
{
    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        if(Input.GetKey(KeyCode.W))
        {
            transform.position += new Vector3(0f, 0.1f);
        }
        if(Input.GetKey(KeyCode.A))
        {
            transform.position += new Vector3(-0.1f, 0f);
        }
        if(Input.GetKey(KeyCode.S))
        {
            transform.position += new Vector3(0f, -0.1f);
        }
        if(Input.GetKey(KeyCode.D))
        {
            transform.position += new Vector3(0.1f, 0f);
        }
    }
}