using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class LerpTest : MonoBehaviour
{
    public Transform endPoint;
    public float amountOfTime = 12f;

    private float elapsedTime = 0f;
    private Vector3 startPos;
    // Update is called once per frame

    private void Start()
    {
        startPos = transform.position;
    }

    void Update()
    {
        transform.position = Vector3.Lerp(startPos, endPoint.position, elapsedTime / amountOfTime);
        elapsedTime += Time.deltaTime;
    }
}