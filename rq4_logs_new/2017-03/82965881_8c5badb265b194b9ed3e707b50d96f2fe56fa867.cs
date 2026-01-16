using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class GatherQuest : Quest {

    public int targetAmount;
    public bool status;
    public int reward;

	public GatherQuest(int t, bool s, int r)
    {
        targetAmount = t;
        status = s;
        reward = r;
    }
}