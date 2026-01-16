using System.Collections;
using System.Collections.Generic;
using System.Threading;
using UnityEngine;

public class MuliTimer
{
    private float time;
    private float elapsedtime;

    public MuliTimer(float limit)
    {
        this.time = limit;
        this.elapsedtime= 0;
    }

    public void reset()
    {
        elapsedtime = 0;
    }

    public bool isDone()
    {
        return time > elapsedtime;
    }

    public void update(float increment)
    {
        elapsedtime+= increment;
    }

}