using UnityEngine;
using System.Collections;
using System;

public class ScheduleEnglish : Schedule
{
    public override void Init()
    {
        base.Init();
        type = ScheduleType.English;
    }

    public override void Effect(Schedule obj)
    {
        Debug.Log("English Effected");
        GameManager.Instance.SetParameter("Stress", 
            GameManager.Instance.GetParameter("Stress") + 1);
        GameManager.Instance.SetParameter("English",
            GameManager.Instance.GetParameter("English") + 1);
    }
}