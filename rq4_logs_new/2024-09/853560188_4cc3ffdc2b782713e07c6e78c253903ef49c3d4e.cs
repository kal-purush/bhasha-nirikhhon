using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using System;

public class TimeManager : MonoBehaviour
{
    public static Action onTimeChange;
    public static Action onDayChange;

    public static int time { get; private set; }
    public static int day { get; private set; }

    public string currTime;


    // Start is called before the first frame update
    void Start()
    {
        time = 0;
        day = 1;
        
    }

    // Update is called once per frame
    void Update()
    {
        if (time == 0)
            currTime = "Morning";
        else if (time == 1)
            currTime = "Afternoon";
        else
            currTime = "Night";

        if(time > 2)
        {
            time = 0;
            day++;
        }
        
    }
}