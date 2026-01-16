using UnityEngine;
using System.Collections;

public class MovingLever : LeverBase
{
    public Vector3 moveAmount = new Vector3(0, 0, -1);
    public float moveSpeed = 1f;
    public float delay = 2f; 
    private Vector3 initialPosition;
    private Vector3 targetPosition;
    private bool isReturning = false;
    private bool isCoroutineRunning = false;

    void Start()
    {
        initialPosition = transform.position;
        targetPosition = initialPosition + moveAmount;
    }

    void Update()
    {
        if (isActive)
        {
            transform.position = Vector3.MoveTowards(transform.position, targetPosition, Time.deltaTime * moveSpeed);
            if (Vector3.Distance(transform.position, targetPosition) < 0.01f)
            {
                isActive = false;
                if (!isCoroutineRunning)
                {
                    StartCoroutine(StartReturn());
                }
            }
        }
        else if (isReturning)
        {
            transform.position = Vector3.MoveTowards(transform.position, initialPosition, Time.deltaTime * moveSpeed);
            if (Vector3.Distance(transform.position, initialPosition) < 0.01f)
            {
                isReturning = false;
            }
        }
    }

    IEnumerator StartReturn()
    {
        isCoroutineRunning = true;
        yield return new WaitForSeconds(delay);
        isReturning = true;
        isCoroutineRunning = false;
    }

    public override void Activate()
    {
        if (!isActive && !isReturning)
        {
            isActive = true;
        }
    }
}