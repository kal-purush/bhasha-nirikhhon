using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class MP_Crash : MonoBehaviour {

    //checks if the player has crashed
    bool isCrashed;

    [SerializeField] float speedUpRecovery;

    //accessing scripts
    MP_ChangeLanes LaneSwitch;
    MP_GroundControl GroundCon;

	// Use this for initialization
	void Start () {

        LaneSwitch = GetComponent<MP_ChangeLanes>();
        GroundCon = GetComponent<MP_GroundControl>();
	}
	
	// Update is called once per frame
	void Update () {
        //works with ResetRot to finish rotation
        if (isCrashed)
        {
            GroundCon.DecreaseRot();
            //the button you mash to speed up the timer after a crash
            if (Input.GetKeyDown(KeyCode.RightArrow))
            {
                LaneSwitch.SpeedUpCrashTime();
            }
        }
	}

    //toggles the boolean isCrashed
    public void ChangeIsCrashed()
    {
        isCrashed = !isCrashed;
        //which toggles another boolean
        if (isCrashed)
        {
            LaneSwitch.ChangeNeedTopLane();
        }
        else
        {
            LaneSwitch.ChangeNeedTopLane();
        }
    }

    //for checking if the boolean is true of false in other scripts
    public bool GetIsCrashed()
    {
        return isCrashed;
    }

    //for speeding up the timer when you crash to recover quicker
    public float GetSpeedUpTime()
    {
        return speedUpRecovery;
    }
}