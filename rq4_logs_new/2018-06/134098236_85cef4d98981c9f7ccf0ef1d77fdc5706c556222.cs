using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class NewPlayerMovement : MonoBehaviour
{
    public float speed = 0.1f;
    public float maxspeed = 0.5f;
    public float acceleration = 0.08f;
    public float secondspressed = 0f;
    public float secondspressedleft = 0f;
    public float speedleft = -0.1f;
    public float maxspeedleft = -0.5f;
    // Use this for initialization
    void Start()
    {

    }

    // Update is called once per frame
    void Update()
    {
        PlayerMove();
    }
    void PlayerMove()
    {
    float truespeed = speed + (acceleration * secondspressed);
    float truespeedback = -(speed + (acceleration * secondspressedleft));
        if (Input.GetKey(KeyCode.D))
        {
            secondspressed += .166f;
            if (Input.GetKey(KeyCode.A))
            {
                secondspressed = 0;
            }
            if (truespeed < maxspeed)
            {
            gameObject.transform.Translate(new Vector2(speed + (acceleration * secondspressed), 0));
            }
            else
            {
                gameObject.transform.Translate(new Vector2(maxspeed, 0));
            }
            
        }
        else if (Input.GetKey(KeyCode.A))
        {
            secondspressedleft += .166f;
            if (Input.GetKey(KeyCode.D))
            {
                secondspressedleft = 0;
            }
            if (truespeedback > -(maxspeed))
            {
                gameObject.transform.Translate(new Vector2(-(speed + (acceleration * secondspressedleft)), 0));
            }
            else
            {
                gameObject.transform.Translate(new Vector2(-maxspeed, 0));
            }
        }
        else
        {
            secondspressed = 0;
            secondspressedleft = 0;
        }
        
    }

}