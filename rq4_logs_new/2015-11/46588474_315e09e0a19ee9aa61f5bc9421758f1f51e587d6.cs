using UnityEngine;
using System.Collections;
using System;
using System.Collections.Generic;


public class Exit : Behavior
{
    // Use this for initialization
    void Start()
    {
        tests = new List<assocMoods>();
        assocMoods tmp = new assocMoods();
        // petite gruge
        tmp.value = -1000; tmp.moodType = "Calm"; tmp.comparison = operations.SUP;
        tests.Add(tmp);
    }

    protected override void Fail(Collider2D coll)
    {
        throw new NotImplementedException();
    }

    protected override void Success(Collider2D coll)
    {
        Memory.Instance.SetStatistics(coll.GetComponent<State>().GetMoods());
        Destroy(coll.gameObject);
    }
}