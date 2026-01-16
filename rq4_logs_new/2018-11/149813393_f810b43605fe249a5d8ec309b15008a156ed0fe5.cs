using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using Fungus;
using UnityEngine.UI;

public class HopeLevelInterlude : MonoBehaviour {

    public Flowchart myFlowchart;
    private float mentalStateScale;
    private bool midStateStatus;
    private bool lowStateStatus;
    private bool hiStateStatus;
    private bool ultraHiStateStatus;
    private bool ultraLowStateStatus;

    public float ultraLowStateThreshold;
    public float ultraHighStateThreshold;
    public float mentalStateLowThreshold;
    public float mentalStateHiThreshold;

    public Text mentalStateText;

    public Slider playerHealthSlider;
    public Text playerHealthText;
    public Slider manaSlider;
    public Text manaText;

    // Use this for initialization
    void Start () {
		
	}
	
	// Update is called once per frame
	void Update () {
        //gets current variable values from the flowchart
        mentalStateScale = myFlowchart.GetFloatVariable("mentalStateScale");

        //checks the hope state and updates bools in script
        if (mentalStateScale < mentalStateLowThreshold && mentalStateScale > ultraLowStateThreshold)
        {
            lowStateStatus = true;
            midStateStatus = false;
            hiStateStatus = false;
            ultraHiStateStatus = false;
            ultraLowStateStatus = false;
            mentalStateText.text = "Disheartened";

        }
        else if (mentalStateScale > mentalStateHiThreshold && mentalStateScale < ultraHighStateThreshold)
        {
            lowStateStatus = false;
            midStateStatus = false;
            hiStateStatus = true;
            ultraHiStateStatus = false;
            ultraLowStateStatus = false;
            mentalStateText.text = "Careless";
        }
        else if (mentalStateScale > ultraHighStateThreshold)
        {
            lowStateStatus = false;
            midStateStatus = false;
            hiStateStatus = false;
            ultraHiStateStatus = true;
            ultraLowStateStatus = false;
            mentalStateText.text = "Very Careless";
        }
        else if (mentalStateScale < ultraLowStateThreshold)
        {
            lowStateStatus = false;
            midStateStatus = false;
            hiStateStatus = false;
            ultraHiStateStatus = false;
            ultraLowStateStatus = true;
            mentalStateText.text = "Very Disheartened";
        }
        else
        {
            lowStateStatus = false;
            midStateStatus = true;
            hiStateStatus = false;
            ultraHiStateStatus = false;
            ultraLowStateStatus = false;
            mentalStateText.text = "Determined";
        }

        //updates flowchart variables to match script variables
        myFlowchart.SetBooleanVariable("midStateStatus", midStateStatus);
        myFlowchart.SetBooleanVariable("lowStateStatus", lowStateStatus);
        myFlowchart.SetBooleanVariable("hiStateStatus", hiStateStatus);
        myFlowchart.SetBooleanVariable("ultraLowStateStatus", ultraLowStateStatus);
        myFlowchart.SetBooleanVariable("ultraHighStateStatus", ultraHiStateStatus);
    }


    public void SetSliders(){
        playerHealthSlider.value = Mathf.MoveTowards(50f, myFlowchart.GetFloatVariable("playerHealthCurrent"), 100f);
        manaSlider.value = Mathf.MoveTowards(50f, myFlowchart.GetFloatVariable("manaCurrent"), 100f);
        manaText.text = myFlowchart.GetFloatVariable("manaCurrent") + "/50";
        playerHealthText.text = myFlowchart.GetFloatVariable("playerHealthCurrent") + "/50";
    }

}