using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class UmbrellaMovement : MonoBehaviour
{
    public Transform[] waypoints;
    public int speed;

    int waypointIndex;
    float dist;

    // Start is called before the first frame update
    void Start()
    {
        waypointIndex = 0;
    }

    // Update is called once per frame
    void FixedUpdate()
    {
        dist = Vector3.Distance(transform.position, waypoints[waypointIndex].position);
        if(dist < 1f)
        {
            UpdateIndex();
        }
        Patrol();
    }

    void Patrol()
    {
        Vector3 directionVector = waypoints[waypointIndex].position - transform.position;
        Vector3 dir = directionVector.normalized;
        transform.Translate(dir * speed * Time.deltaTime);
    }

    void UpdateIndex()
    {
        waypointIndex = (waypointIndex + 1) % waypoints.Length;
    }
}