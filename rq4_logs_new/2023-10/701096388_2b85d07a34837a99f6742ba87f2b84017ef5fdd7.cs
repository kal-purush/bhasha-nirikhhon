using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlayerController : MonoBehaviour
{

    public float speed = 0.05f;
    private float inputX;
    private float inputY;

    // Start is called before the first frame update
    void Start()
    {
        inputX = 0; inputY = 0;
    }

    // Update is called once per frame
    void Update()
    {
        if (Input.GetKey(KeyCode.W))
        {
            inputY += speed;
        }
        if (Input.GetKey(KeyCode.S))
        {
            inputY -= speed;
        }
        if (Input.GetKey(KeyCode.A))
        {
            inputX -= speed;
        }
        if (Input.GetKey(KeyCode.D))
        {
            inputX += speed;
        }


        Vector2 movement = new Vector2(inputX, inputY);
        //movement *= Time.deltaTime;
        transform.Translate(movement);
        inputX = 0;
        inputY = 0;
    }
}