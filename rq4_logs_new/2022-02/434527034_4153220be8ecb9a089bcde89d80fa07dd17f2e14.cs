using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class BasicEnemyAI : MonoBehaviour
{
    public float moveSpeed;
    public float moveDistance;
    private bool hasStartedMoving = false;
    [SerializeField] private float startX;
    
    void Start()
    {
        startX = transform.position.x;
    }

    void Update()
    {
        if(!hasStartedMoving)
        {
            transform.Translate(Vector2.left * moveSpeed * Time.deltaTime);
        }

        if((transform.position.x - startX) <= -moveDistance)
        {
            transform.Translate(Vector2.left * moveSpeed * Time.deltaTime);
            transform.Rotate(0f, 180f, 0f);
        }
        if((transform.position.x - startX) >= moveDistance)
        {
            transform.Translate(Vector2.right * moveSpeed * Time.deltaTime);
            transform.Rotate(0f, 180f, 0f);
        }
    }
}