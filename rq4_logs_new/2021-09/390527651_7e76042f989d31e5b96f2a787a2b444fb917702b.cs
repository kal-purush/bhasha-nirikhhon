using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class TimeManager : MonoBehaviour
{
    private const float tickTimerMax = 1.0f;
    private const int maxTicks = 1440;
    private int tick;
    private float tickTimer;
    public int currentTick;
    public int seconds;
    public int hours;
    public int days;

    //This will determine where the tick will start before counting up.
    //This will allow saves of the current tick in future so the game can be started at a specific time.
    void Awake()
    {
        tick = currentTick;

        seconds = tick % 60;
        hours = (currentTick - (currentTick % 60)) / 60;
    }

    void FixedUpdate()
    {
        //This will reset the ticks backs to zero after they reach maxTick value.
        if (tick == maxTicks)
        {
            tick = 0;
            days++;
            hours = 0;
        }

        //This will update the tick every second (to change time they are updated, update the tickTimer value).
        tickTimer += Time.deltaTime;
        if (tickTimer > tickTimerMax)
        {
            if (seconds == 60)
            {
                hours++;
                seconds = 0;
            }

            tickTimer -= tickTimerMax;
            tick++;
            seconds++;
        }
    }
}