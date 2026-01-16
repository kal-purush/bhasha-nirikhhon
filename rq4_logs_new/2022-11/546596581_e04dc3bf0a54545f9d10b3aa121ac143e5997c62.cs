using System.Collections;
using System.Collections.Generic;
using System.Threading;
using UnityEngine;

public class EnemyFlying : MonoBehaviour
{
    public Rigidbody2D body;


    private Vector2 randomposition;

    public float movespeed = 0.33f;
    
    private float speed;
    private float distance;
    private float time = 0.33f;
    private float moveTimer;
    void Move(float diffx, float diffy, float time)
    {
        
        
        distance = Mathf.Sqrt(diffx * diffx + diffy * diffy);
        
        moveTimer = time;
        if (moveTimer > 0)
        {
            body.velocity = new Vector2(body.transform.position.x + diffx, body.transform.position.y + diffy) * (distance / time);
            moveTimer -= Time.deltaTime;
        }

    }

    private void Update()
    {
        randomposition = new Vector2(Random.value * 2, Random.value * 2);
        Move(randomposition.x, randomposition.y, movespeed);
    }
}