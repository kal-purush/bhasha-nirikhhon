using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class GravityEventManager : MonoBehaviour
{
    public GravityInvertEvent onGravityInvert;

    public float minTime = 5f;
    public float maxTime = 10f;

    void Start()
    {
        StartCoroutine(InvokeGravityInvertRandomly());
    }

    IEnumerator InvokeGravityInvertRandomly()
    {
        yield return new WaitForSeconds(Random.Range(minTime, maxTime));
        bool isGravityInverted = Physics.gravity.y < 0;
        Physics.gravity = -Physics.gravity;
        onGravityInvert.Invoke(isGravityInverted);
        StartCoroutine(InvokeGravityInvertRandomly());
    }
}
