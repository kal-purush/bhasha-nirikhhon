using UnityEngine;
using System.Collections;
using System.Collections.Generic;


public class TreeBehavior : Behavior {

    protected bool waiting = false;

	// Use this for initialization
	void Start () {
        isActive = true;
        tests = new List<assocMoods>();
        assocMoods tmp = new assocMoods();
        tmp.value = 30; tmp.moodType = "Empowerment"; tmp.comparison = operations.INF;
        tests.Add(tmp);
    }
	
	// Update is called once per frame
	void Update () {
	
	}

    protected override void Success(Collider2D coll)
    {
        coll.GetComponent<Movement>().Pause(1);
        coll.GetComponent<State>().ModifyValue("Calm", 30);
        gameObject.GetComponent<Collider2D>().enabled = false;
    }

    protected override void Fail(Collider2D coll)
    {
        coll.GetComponent<State>().ModifyValue("Calm", -20);
        BlockFacotry.Instance.GiveTree(gameObject);
    }

}