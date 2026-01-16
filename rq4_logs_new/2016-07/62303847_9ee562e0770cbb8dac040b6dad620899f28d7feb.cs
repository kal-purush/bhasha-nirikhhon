using UnityEngine;
using System.Collections;
using System;

public class ScheduleBasicMath1 : Schedule
{

	// Use this for initialization
	void Start () {
        type = ScheduleType.BasicMath;
	}

    public override void Effect(Schedule obj)
    {
        GameManager.Instance.SetParameter("Stress", GameManager.Instance.GetParameter("Stress") + 1);
        GameManager.Instance.SetParameter("BasicMath1", GameManager.Instance.GetParameter("BasicMath1") + 1);
        Debug.Log("Take a rest Effected!");
    }
}