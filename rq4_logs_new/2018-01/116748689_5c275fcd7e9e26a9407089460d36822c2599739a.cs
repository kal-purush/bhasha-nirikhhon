using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using System;

public class MP_ChangeLanes : MonoBehaviour {

    //sets up the lanes as states
    enum LaneStates
    {
        ONE,//bottom lane
        TWO,//second lane up
        THREE,//third lane up
        FOUR,//fourth lane up

        NUM_STATES
    }

    //sets current state as lane three(second from the top)
    LaneStates curState = LaneStates.THREE;
    Dictionary<LaneStates, Action> fsm = new Dictionary<LaneStates, Action>();

    //the heights of the four different lanes
    float heightOne = -1.4f;
    float heightTwo = -0.4f;
    float heightThree = 0.6f;
    float heightFour = 1.6f;
    //used to change the player's height(changing lanes)
    Vector3 curPos;

	// Use this for initialization
	void Start () {
        //adding the four states to the dictionary
        fsm.Add(LaneStates.ONE, new Action(StateOne));
        fsm.Add(LaneStates.TWO, new Action(StateTwo));
        fsm.Add(LaneStates.THREE, new Action(StateThree));
        fsm.Add(LaneStates.FOUR, new Action(StateFour));
        SetState(LaneStates.THREE);

        curPos = transform.position;
        
    }

    // Update is called once per frame
    void Update() {
        //continues to call current state every frame
        fsm[curState].Invoke();
        //sets the current position every frame
        transform.SetPositionAndRotation(curPos, transform.rotation);

    }

    //called in controls, moves the player up a lane
    public void MoveUp()
    {
        //prevents lane from going above four
        if(curState == LaneStates.FOUR)
        {
            curState = LaneStates.FOUR;
        }
        else
        //moves lane up one
        { 
            curState++;
        }
        
    }

    public void MoveDown()
    {
        //prevents lane from going below one
       if(curState == LaneStates.ONE)
        {
            curState = LaneStates.ONE;
        }
       //moves lane down one
       else
        {
            curState--;
        }
        
    }

    //sets the height for each lane
    void StateOne()
    {
        curPos = new Vector3(-32.984f, heightOne, -2);
        
    }

    void StateTwo()
    {
        curPos = new Vector3(-32.984f, heightTwo, -2);
        
    }
    
    void StateThree()
    {
        curPos = new Vector3(-32.984f, heightThree, -2);
        
    }

    void StateFour()
    {
        curPos = new Vector3(-32.984f, heightFour, -2);
        
    }
   
    void SetState(LaneStates newState)
    {
        curState = newState;
    }
}