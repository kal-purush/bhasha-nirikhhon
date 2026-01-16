using UnityEngine;
using System.Collections;
using System.Collections.Generic;
using System;

public class ToolBehavior : Behavior {

	// Use this for initialization
	void Start () {
        tests = new List<assocMoods>();
        assocMoods tmp = new assocMoods();
        tmp.value = 70; tmp.moodType = "Empowerment"; tmp.comparison = operations.INF;
        tests.Add(tmp);
    }
	
	// Update is called once per frame
	void Update () {
	
	}

    protected override void Success(Collider2D coll)
    {
        Debug.Log("success");
        coll.GetComponent<State>().GrabTool();
        BlockFacotry.Instance.GiveTool(gameObject);
    }
    protected override void Fail(Collider2D coll)
    {
        BlockFacotry.Instance.GiveTool(gameObject);
    }

}