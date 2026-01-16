using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class MP_Overheat : MonoBehaviour {

    bool isOverheated;

    MP_ChangeLanes LaneSwitch;
    MP_Boost BoostScript;

	// Use this for initialization
	void Start () {
        LaneSwitch = GetComponent<MP_ChangeLanes>();
        BoostScript = GetComponent<MP_Boost>();
	}
	
	// Update is called once per frame
	void Update () {

        if (isOverheated)
        {
            BoostScript.StopBoost();
        }
	}

    public void ChangeIsOverheated()
    {
        isOverheated = !isOverheated;
        if (isOverheated)
        {
            LaneSwitch.ChangeNeedTopLane();
        }
        else
        {
            LaneSwitch.ChangeNeedTopLane();
        }
    }

    public bool GetIsOverheated()
    {
        return isOverheated;
    }
}