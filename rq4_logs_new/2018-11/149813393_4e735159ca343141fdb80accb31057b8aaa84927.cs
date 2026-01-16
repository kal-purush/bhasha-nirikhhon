using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using Fungus;

public class IntroSceneManagement : MonoBehaviour {
   
    private float totalChimeraHealth;
    public float totalChimeraHealthThreshold;
    public Flowchart myFlowchart;
    public int introSceneLength;

    // Use this for initialization
    void Start () {
		
	}
	
	// Update is called once per frame
	void Update () {
        totalChimeraHealth = myFlowchart.GetFloatVariable("chimeraHealthCurrent") + myFlowchart.GetFloatVariable("ramHealthCurrent") + myFlowchart.GetFloatVariable("dragonHealthCurrent");


        //add conditional for ending the scene
        if ((myFlowchart.GetBooleanVariable("playerIsAttacking") == true) && (myFlowchart.GetIntegerVariable("playerSpellNumber") == introSceneLength || myFlowchart.GetFloatVariable("playerHealthCurrent") <= 10f || totalChimeraHealth <= totalChimeraHealthThreshold))
        {
            myFlowchart.StopAllBlocks();
            myFlowchart.ExecuteBlock("end scene (Copy) (Copy)");
        }
    }
}