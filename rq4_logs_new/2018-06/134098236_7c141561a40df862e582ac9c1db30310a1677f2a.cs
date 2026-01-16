using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class EnemyMovement : MonoBehaviour
{

    public Rigidbody2D rb;

    public float moveVel;

    private bool reverse;

    private Vector3 vel;

    // Use this for initialization
    void Start()
    {

    }

    // Update is called once per frame
    void Update()
    {

        if (transform.position.x >= 7.55f)
        {
            reverse = true;
        }
        else if (transform.position.x <= -7.55f)
        {
            reverse = false;
        }

        if (reverse)
        {
            RaycastHit2D playerDetectorReverse = Physics2D.Raycast(transform.position, -Vector2.right, 7f, LayerMask.GetMask("Player"));
            if (playerDetectorReverse.collider == null)
            {
                vel.x = -moveVel;
            }
            else if (playerDetectorReverse.collider.CompareTag("Player"))
            {
                moveVel = -.18f;
                vel.x = moveVel;
            }
        }
        else
        {
            RaycastHit2D playerDetector = Physics2D.Raycast(transform.position, Vector2.right, 7f, LayerMask.GetMask("Player"));
            if (playerDetector.collider == null)
            {
                vel.x = moveVel;
            }
            else if (playerDetector.collider.gameObject.CompareTag("Player"))
            {
                moveVel = .18f;
                vel.x = moveVel;
            }
        }


        rb.MovePosition(transform.position + vel);

    }
}