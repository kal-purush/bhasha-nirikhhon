using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using System;

public class MP_Temp : MonoBehaviour
{

    //sets up the lanes as states
    enum LaneStates
    {
        ONE,//bottom lane
        TWO,//second lane up
        THREE,//third lane up
        FOUR,//fourth lane up
        RECLANE//where the player goes after crashing or overheating

    }

    //sets current state as lane three(second from the top)
    [SerializeField] LaneStates curState = LaneStates.ONE;
    [SerializeField] LaneStates prevState;
    Dictionary<LaneStates, Action> fsm = new Dictionary<LaneStates, Action>();

    //the heights of the four different lanes
    [SerializeField] float heightOne = -1.4f;
    [SerializeField] float heightTwo = -0.4f;
    [SerializeField] float heightThree = 0.6f;
    [SerializeField] float heightFour = 1.6f;
    [SerializeField] float heightRec = 2.6f;
    float curHeight;

    //speed at which the player changes lanes
    [SerializeField] float speed;

    //used to change the player's height(changing lanes)
    Vector3 curPos;

    //stops the state change from being called back to back
    [SerializeField] float timer = 0.2f;
    [SerializeField] float timePassed;
    //secondary timer for when you crash
    [SerializeField] float crashTimer;

    //checks if the player needs to go to the lane above four for overheating or crashing
    bool needTopLane = false;

    //accessing scripts
    MP_Crash Crashing;
    MP_Controls Controls;
    MP_Acceleration Accel;
    MP_GroundControl GroundCon;
    MP_Boost BoostScript;
    MP_Overheat Heating;
    MP_Ramping Ramps;

    // Use this for initialization
    void Start()
    {
        //adding the four states to the dictionary
        fsm.Add(LaneStates.ONE, new Action(StateOne));
        fsm.Add(LaneStates.TWO, new Action(StateTwo));
        fsm.Add(LaneStates.THREE, new Action(StateThree));
        fsm.Add(LaneStates.FOUR, new Action(StateFour));
        fsm.Add(LaneStates.RECLANE, new Action(StateRec));

        SetState(LaneStates.ONE);

        curPos = transform.position;

        Crashing = GetComponent<MP_Crash>();
        Controls = GetComponent<MP_Controls>();
        Accel = GetComponent<MP_Acceleration>();
        GroundCon = GetComponent<MP_GroundControl>();
        BoostScript = GetComponent<MP_Boost>();
        Heating = GetComponent<MP_Overheat>();
        Ramps = GetComponent<MP_Ramping>();
    }

    // Update is called once per frame
    void Update()
    {
        //continues to call current state every frame
        fsm[curState].Invoke();
        curPos = new Vector3(-32.984f, curHeight, -2);
        //moves the player to the newest position every frame
        transform.position = Vector3.Lerp(transform.position, curPos, speed * Time.deltaTime);


        //timer
        timePassed += Time.deltaTime;

        //checks if player has crashed OR overheated
        if (needTopLane)
        {
            //slowly moves player to the recovery lane
            if (timePassed > timer && curState != LaneStates.RECLANE)
            {
                //disables player input, it needs both disabled for some reason
                Controls.enabled = false;
                Accel.enabled = false;

                //looping timer and state change
                timePassed = 0;
                curState++;

            }
        }

    }

    public void OverheatTime()
    {
        crashTimer = BoostScript.GetOverheatTime();
    }

    //used to set the timer to the crash time for a wheelie
    public void WheelieCrash()
    {
        crashTimer = GroundCon.GetWheelieTime();
    }

    //used by MP_Crash to speed up the timer during a crash
    public void SpeedUpCrashTime()
    {
        timePassed = timePassed + Crashing.GetSpeedUpTime();
    }

    //toggles boolean
    public void ChangeNeedTopLane()
    {
        needTopLane = !needTopLane;
    }

    //for checking true or false in other scripts
    public bool GetNeedTopLane()
    {
        return needTopLane;
    }

    //called in controls, moves the player up a lane
    public void MoveUp()
    {

        //prevents lane from going above four
        //moves lane up one
        if (curState != LaneStates.FOUR && timePassed > timer)
        {
            //resetting timer then switching states
            timePassed = 0;
            curState++;

        }

    }

    public void MoveDown()
    {

        //prevents lane from going below one
        //moves lane down one
        if (curState != LaneStates.ONE && timePassed > timer)
        {
            //resetting timer then switching states
            timePassed = 0;
            curState--;

        }
    }

    //sets the height for each lane
    void StateOne()
    {
        curHeight = heightOne;
        Ramps.LaneOne();
    }

    void StateTwo()
    {
        curHeight = heightTwo;
        Ramps.LaneTwo();
    }

    void StateThree()
    {
        curHeight = heightThree;
        Ramps.LaneThree();
    }

    void StateFour()
    {
        curHeight = heightFour;
        Ramps.LaneFour();
    }

    void StateRec()
    {
        curHeight = heightRec;

        //insert if check for overheating
        if (Heating.GetIsOverheated())
        {
            if (timePassed > crashTimer)
            {
                timePassed = 0;
                Heating.ChangeIsOverheated();
                curState = LaneStates.FOUR;
                Controls.enabled = true;
                Accel.enabled = true;
            }
        }

        //checking if the reason for going to this state if for crashing
        if (Crashing.GetIsCrashed())
        {
            //keeps player in recovery lane for set amount of time
            if (timePassed > crashTimer)
            {
                //resets timer, toggles isCrashed, changes lane to the top lane, reenables input
                timePassed = 0;
                Crashing.ChangeIsCrashed();
                curState = LaneStates.FOUR;
                Controls.enabled = true;
                Accel.enabled = true;
            }
        }
    }

    public void ExitRampState()
    {
        if (Ramps.GetLaneNumber() == 0)
        {
            curState = LaneStates.ONE;
        }
        if (Ramps.GetLaneNumber() == 1)
        {
            curState = LaneStates.TWO;
        }
        if (Ramps.GetLaneNumber() == 2)
        {
            curState = LaneStates.THREE;
        }
        if (Ramps.GetLaneNumber() == 3)
        {
            curState = LaneStates.FOUR;
        }
    }

    void SetState(LaneStates newState)
    {
        curState = newState;
    }
}