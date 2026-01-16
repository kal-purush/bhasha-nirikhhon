using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class MovingObstacle : MonoBehaviour
{
    [Tooltip("Movement range from the position of start to end.")]
    [SerializeField] private Transform start, end;
    private Transform obstacle;
    [Tooltip("Speed of obstacle.")]
    [SerializeField] private float moveSpeed;
    private bool reverse;

    private void Awake()
    {
        obstacle = transform;
    }

    private void Update()
    {
        CheckReverse();
        MoveObstacle();
    }

    // Checks obstacle position and ticks reverse boolean correspondingly
    private void CheckReverse()
    {
        if (obstacle.position == end.position)
        {
            reverse = true;
        }

        if (obstacle.position == start.position)
        {
            reverse = false;
        }
    }

    // Moves obstacle forwards or reversed
    private void MoveObstacle()
    {
        if (reverse)
        {
            obstacle.position = Vector3.MoveTowards(obstacle.position, start.position, moveSpeed * Time.deltaTime);
        }
        else
        {
            obstacle.position = Vector3.MoveTowards(obstacle.position, end.position, moveSpeed * Time.deltaTime);
        }
    }

    // Path visualization
    private void OnDrawGizmos()
    {
        Gizmos.DrawLine(start.position, end.position);
    }
}