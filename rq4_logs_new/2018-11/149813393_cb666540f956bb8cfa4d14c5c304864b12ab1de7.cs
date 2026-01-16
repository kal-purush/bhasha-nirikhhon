using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using Fungus;

public class StateTracker : MonoBehaviour {

    private string previousState;
    private string currentState;
    private int hiStateConsecutive = 0;
    private int lowStateConsecutive = 0;

    [Header("# of consecutive turns needed in each state to change the damage type")]
    public int lowStateElementalThreshold;
    public int hiStateElementalThreshold;

    public Flowchart myFlowchart;


    // Use this for initialization
	void Start () {
        previousState = "none";

	}
	
	// Update is called once per frame
	void Update () {

		
	}

    //This function is called once at the start of the player's turn
    public void TrackHopeState(){

        //determines current status
        //CURRENTLY DOES NOT DIFFERENTIATE EXTREME VERSIONS OF HIGH AND LOW STATES
        if(myFlowchart.GetBooleanVariable("lowStateStatus") == true || myFlowchart.GetBooleanVariable("ultraLowStateStatus") == true)
        {
            currentState = "lowHope";
        }
        else if(myFlowchart.GetBooleanVariable("midStateStatus") == true)
        {
            currentState = "midHope";
        }
        else if(myFlowchart.GetBooleanVariable("hiStateStatus") == true || myFlowchart.GetBooleanVariable("ultraHighStateStatus") == true)
        {
            currentState = "hiHope";
        }


        //compare to the previous one
        //CURRENTLY ONLY RELEVANT TO TRACKING TIME IN LOW STATE AND HIGH STATE
        //if it matches, increment an int that gets passed into the flowchart
        //if it doesn't, set the int to 0 and pass that to the flowchart
        if(currentState == "lowHope" && previousState == "lowHope"){
            lowStateConsecutive += 1;
            myFlowchart.SetIntegerVariable("lowHopeTurnsConsecutive", lowStateConsecutive);
            //update a bool in the flowchart if the threshold is reached
            if (lowStateConsecutive >= lowStateElementalThreshold)
            {
                myFlowchart.SetBooleanVariable("lowHopeElementalCharged", true);
            }
        }
        else if (previousState == "lowHope" && currentState !="lowHope"){
            lowStateConsecutive = 0;
            myFlowchart.SetIntegerVariable("lowHopeTurnsConsecutive", lowStateConsecutive);
        }
        else if(currentState == "hiHope" && previousState == "hiHope"){
            hiStateConsecutive += 1;
            myFlowchart.SetIntegerVariable("hiHopeTurnsConsecutive", hiStateConsecutive);
            //update a bool in the flowchart if the threshold is reached
            if (hiStateConsecutive >= hiStateElementalThreshold){
                myFlowchart.SetBooleanVariable("hiHopeElementalCharged", true);
            }
        }
        else if (previousState == "hiHope" && currentState != "hiHope"){
            hiStateConsecutive = 0;
            myFlowchart.SetIntegerVariable("hiHopeTurnsConsecutive", hiStateConsecutive);
        }
        else if (currentState == "midHope"){
            hiStateConsecutive = 0;
            myFlowchart.SetIntegerVariable("hiHopeTurnsConsecutive", hiStateConsecutive);
            lowStateConsecutive = 0;
            myFlowchart.SetIntegerVariable("lowHopeTurnsConsecutive", lowStateConsecutive);
        }

        //every time the player becomes determined after being in LOW HOPE
        //check to see if the damage type should be changed based on how much time they spent in the other state
        //then reset the charge bool
        //any damage type that previously applied will be replaced
        if (currentState == "midHope" && previousState =="lowHope" && myFlowchart.GetBooleanVariable("lowHopeElementalCharged") == true)
        {
                myFlowchart.SetBooleanVariable("damageTypeLow", true);
                myFlowchart.SetBooleanVariable("damageTypeHigh", false);
                myFlowchart.SetBooleanVariable("damageTypeNormal", false);
                myFlowchart.SetBooleanVariable("lowHopeElementalCharged", false);
        }
        //same for HIGH HOPE
        if (currentState == "midHope" && previousState == "hiHope" && myFlowchart.GetBooleanVariable("hiHopeElementalCharged") == true)
        {
            myFlowchart.SetBooleanVariable("damageTypeLow", false);
            myFlowchart.SetBooleanVariable("damageTypeHigh", true);
            myFlowchart.SetBooleanVariable("damageTypeNormal", false);
            myFlowchart.SetBooleanVariable("hiHopeElementalCharged", false);
        }


        //LASTLY update the "previous" state to the current state to be used for the next comparison
        previousState = currentState;

    }
}