using System.Collections.Generic;
using UnityEngine;

public class NodamageAnimal : MonoBehaviour
{
    public float flightSpeed = 1.5f;         
    private float waypointReachedDistance = 0.2f; 
    public List<Transform> waypoints;

    public float weaveAmplitude = 0.3f;     
    public float weaveFrequency = 4f;      
    public bool useWeaving = true;          
    private float randomOffset = 0f;         

    Animator animator;
    Rigidbody2D rb;

    Transform nextWaypoint;
    int waypointNum = 0;
    private float timeCounter = 0f;

    private void Awake()
    {
        animator = GetComponent<Animator>();
        rb = GetComponent<Rigidbody2D>();
    }

    private void Start()
    {
        nextWaypoint = waypoints[waypointNum];
        randomOffset = Random.Range(0f, 2f); 
    }

    private void FixedUpdate()
    {
        Flight();
        timeCounter += Time.fixedDeltaTime;
    }

    private void Flight()
    {
        Vector2 directionToWaypoint = (nextWaypoint.position - transform.position).normalized;
        Vector2 targetVelocity = directionToWaypoint * flightSpeed;

        if (useWeaving)
        {
            Vector2 perpendicularDir = new Vector2(-directionToWaypoint.y, directionToWaypoint.x);
            float weaveOffset = Mathf.Sin((timeCounter + randomOffset) * weaveFrequency) * weaveAmplitude;

            rb.linearVelocity = targetVelocity;
            Vector2 weavePosition = perpendicularDir * weaveOffset;
            rb.position += weavePosition * Time.fixedDeltaTime * weaveFrequency;
        }
        else
        {
            rb.linearVelocity = targetVelocity;
        }

        UpdateDirection();

        float distance = Vector2.Distance(nextWaypoint.position, transform.position);
        if (distance <= waypointReachedDistance)
        {
            waypointNum++;
            if (waypointNum >= waypoints.Count)
            {
                waypointNum = 0;
            }
            nextWaypoint = waypoints[waypointNum];
            randomOffset = Random.Range(0f, 1f);
        }
    }

    private void UpdateDirection()
    {
        Vector3 locScale = transform.localScale;
        if (transform.localScale.x > 0)
        {
            if (rb.linearVelocity.x < 0)
            {
                transform.localScale = new Vector3(-1 * locScale.x, locScale.y, locScale.z);
            }
        }
        else
        {
            if (rb.linearVelocity.x > 0)
            {
                transform.localScale = new Vector3(-1 * locScale.x, locScale.y, locScale.z);
            }
        }
    }
}