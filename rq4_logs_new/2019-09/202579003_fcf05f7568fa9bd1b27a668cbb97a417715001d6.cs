using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class EnemyMovement : MonoBehaviour
{
    [SerializeField]
    private float moveSpeed;
    // transform pt1 on left
    [SerializeField]
    private Transform point1;
    // transform pt2 on right
    [SerializeField]
    private Transform point2;
    [SerializeField]
    private Vector2 targetPoint;
    private Vector2 GetRandomPosititonBetweenPoints(ref Transform point1, ref Transform point2)
    {
        float yValue = 0f;
        float randomXValue = 0f;
        randomXValue = Random.Range(point1.position.x, point2.position.x);
        return new Vector2(randomXValue, yValue);
    }
    private Vector2 GetNextPoint()
    {
        if (Mathf.Approximately(point1.position.x, targetPoint.x) &&
            Mathf.Approximately(point1.position.y, targetPoint.y))
        {
            targetPoint = point2.transform.position;
            transform.eulerAngles = new Vector3(0, -180, 0);
        }
        else if (Mathf.Approximately(point2.position.x, targetPoint.x) &&
            Mathf.Approximately(point2.position.y, targetPoint.y))
        {
            targetPoint = point1.transform.position;
            transform.eulerAngles = new Vector3(0, 0, 0);
        }
        else
        {
            GetRandomPosititonBetweenPoints(ref point1, ref point2);
        }

        return targetPoint;
    }
    private void Move()
    {
        transform.position = 
            Vector2.MoveTowards(transform.position, targetPoint, moveSpeed * Time.deltaTime);

        if (Mathf.Approximately(transform.position.x, targetPoint.x) &&
            Mathf.Approximately(transform.position.y, targetPoint.y))
        {
            GetNextPoint();
        }


    }
    private void Update()
    {
        Move();
    }
    private void Start()
    {
        targetPoint = point2.position;
        transform.eulerAngles = new Vector3(0, -180, 0);
    }
}