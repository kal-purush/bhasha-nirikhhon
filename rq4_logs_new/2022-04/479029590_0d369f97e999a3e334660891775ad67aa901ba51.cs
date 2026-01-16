using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Player : MonoBehaviour
{
    Rigidbody2D rg;

    float speed = 20f;


    // Start is called before the first frame update
    void Start()
    {
        rg = GetComponent<Rigidbody2D>();
    }

    // Update is called once per frame
    void Update()
    {

        if (Input.GetKeyDown(KeyCode.W))
        {
            rg.AddForce(new Vector2(0 , speed * Time.deltaTime * 100));
        }
        else if (Input.GetKeyDown(KeyCode.S))
        {
            rg.AddForce(new Vector2(0, -speed * Time.deltaTime * 100));
        }
        else if (Input.GetKeyDown(KeyCode.A))
        {
            rg.AddForce(new Vector2(-speed * Time.deltaTime * 100, 0));
        }
        else if (Input.GetKeyDown(KeyCode.D))
        {
            rg.AddForce(new Vector2(speed * Time.deltaTime * 100, 0)); 
        }
        
    }
}