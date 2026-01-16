using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class FlyingEnemyController : MeleeEnemyController
{
    public float movementRange = 10f;
    public float waitTime = .5f;

    int mode = 0;
    float currentWaitTime = 0f;
    Vector2 initialPoint;
    Vector2 targetPosition;

    // Start is called before the first frame update
    void Start()
    {
        base.Start();
        initialPoint = transform.position;
        targetPosition = ChooseNewTargetPoint();
        
    }

    // Update is called once per frame
    void Update()
    {
        float distanceToPlayer = Vector2.Distance(transform.position, player.transform.position);
        if(distanceToPlayer <= aggroRange)
        {
            targetPosition = player.transform.position;
        }

        if(targetPosition == null || Vector2.Distance(targetPosition, transform.position) <= 1)
        {
            targetPosition = ChooseNewTargetPoint();
            currentWaitTime = waitTime;
        }


        if(currentWaitTime <= 0 && invulnerabilityTimer <= 0)
        {
            //face the enemy towards its movement position
            if (targetPosition.x - transform.position.x >= 0)
            {
                spriteRenderer.flipX = true;
            }
            else
            {
                spriteRenderer.flipX = false;
            }
            transform.position = Vector2.MoveTowards(transform.position, targetPosition, speed * Time.deltaTime);
        }

        currentWaitTime -= Time.deltaTime;
    }

    private void OnCollisionEnter2D(Collision2D collision)
    {
        if(collision.gameObject.tag == "Player")
        {
            collision.gameObject.GetComponent<PlayerController>().TakeDamage(10, transform.position, 2);
        }
    }

    Vector2 ChooseNewTargetPoint()
    {
        Vector2 newPoint = Vector2.zero;

        newPoint.x = Random.Range(-movementRange, movementRange);
        newPoint.y = Random.Range(-movementRange, movementRange);

        Debug.Log("New Point: " + newPoint);
        newPoint += initialPoint;

        int groundOnlyMask = LayerMask.GetMask("Ground");
        Vector2 direction = ((Vector2)transform.position - newPoint).normalized;
        RaycastHit2D hit = Physics2D.Raycast(transform.position, direction, Vector2.Distance(newPoint, transform.position), groundOnlyMask);

        if(hit.collider != null)
        {
            Debug.Log("hit: " + hit.collider.gameObject.name);
            return initialPoint;
        }
        else
        {
            return newPoint;
        }
    }
}