using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Fire : MonoBehaviour
{
    [Range(0, 100)]
    public int fireProbability = 10;    // 0% ~ 100%
    public float fireCheckIntervalTime = 2.5f;
    private float fireCheckTimer;

    void Start()
    {
        fireCheckTimer = fireCheckIntervalTime;
    }

    void Update()
    {
        fireCheckTimer -= Time.deltaTime;
        if (fireCheckTimer <= 0)
        {
            fireCheckTimer = fireCheckIntervalTime;
            int rnum = Random.Range(0, 100);
            if (rnum < fireProbability)
                TreesController.instance.StartFire();
        }
    }
}