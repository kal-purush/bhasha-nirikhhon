using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class Scorekeeper : MonoBehaviour {

    int currentScoreBox = 0;

    int pinsDown = 0;

    public Text[] scoreBoxes = new Text[20];


	// Use this for initialization
	void Start () {
		
	}
	
	// Update is called once per frame
	void Update () {
		
	}


    public void NextFrame()
    {
        if (currentScoreBox != 19)
        {
            scoreBoxes[currentScoreBox].text = pinsDown.ToString();
            currentScoreBox++;
            pinsDown = 0;
        }
    }

    public void PinHit()
    {
        pinsDown++;
    }

}