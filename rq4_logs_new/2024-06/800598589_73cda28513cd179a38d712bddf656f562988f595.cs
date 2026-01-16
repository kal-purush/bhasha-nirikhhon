using System;

public class Timer : Processable
{
    protected double time;
    protected Action timeout;
    protected bool isActive;

    public double Time
    {
        get => time;
        set => time = value;
    }

    public Action Timeout
    {
        get => timeout;
        set => timeout = value;
    }

    public bool IsActive
    {
        get => isActive;
        set => isActive = value;
    }

    public void Process(double delta)
    {
        if (!this.isActive)
        {
            return;
        }

        this.time -= delta;
        if (this.time <= 0.0f)
        {
            this.timeout();
            this.isActive = false;
        }
    }
    
    public void Activate(double time)
    {
        this.time = time;
        this.isActive = true;
    }

    public Timer(Action timeout)
    {
        this.timeout = timeout;
    }
}