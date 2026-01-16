using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Typhoon : MonoBehaviour
{
    bool turn = false;
    float moveX = 0;

    // Use this for initialization
    void Start()
    {

    }

    // Update is called once per frame
    void Update()
    {
        if (moveX == 0)
        {
            moveX = Random.Range(-0.5f, 0.5f);
        }
        float speed = Time.deltaTime;
        Vector3 pos = transform.position;
        if (moveX > 0)
        {
            if (turn)
            {
                pos.x -= speed;
                if (pos.x <= 0)
                {
                    pos.x = 0;
                    moveX = 0;
                    turn = false;
                }
            }
            else
            {
                pos.x += speed;
                if (pos.x >= moveX)
                {
                    turn = true;
                }
            }
        }
        else
        {
            if (turn)
            {
                pos.x += speed;
                if (pos.x >= 0)
                {
                    pos.x = 0;
                    moveX = 0;
                    turn = false;
                }
            }
            else
            {
                pos.x -= speed;
                if (pos.x <= moveX)
                {
                    turn = true;
                }
            }
        }

        pos.y -= 1.0f;
        if (pos.y <= -5)
        {
            pos.y = 17;
        }

        transform.position = pos;
    }
}