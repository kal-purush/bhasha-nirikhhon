using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class TimeTick : MonoBehaviour
{
    private const float tickTimerMax = 1.0f;
    private int tick;
    private float tickTimer;
    public int maxTicks;

    private void Awake()
    {
        tick = 0;
    }

    private void Update()
    {
        if (tick == maxTicks)
        {
            tick = 0;
        }

        tickTimer += Time.deltaTime;
        if(tickTimer > tickTimerMax)
        {
            tickTimer -= tickTimerMax;
            tick++;

            GameManager.instance.ShowText("Tick " + tick, 25, Color.red, transform.position, Vector3.up * 50, 3.0f);
        }
    }
}