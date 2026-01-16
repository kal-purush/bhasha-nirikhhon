using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class FightController : MonoBehaviour {


    public string turn = "player";

    public GameObject moveSelector;
    public GameObject statusText;

	// Use this for initialization
	void Start () {
		
	}
	
	// Update is called once per frame
	void Update () {
        updateUI();
	}

    void updateUI() {
        //This function should be called every time the UI needs to be updated.
        if(turn == "player") {
            moveSelector.SetActive(true);
            statusText.SetActive(false);
        } else {
            moveSelector.SetActive(false);
            statusText.SetActive(true);
        }
    }
}