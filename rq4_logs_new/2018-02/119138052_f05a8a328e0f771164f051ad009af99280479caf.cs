using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Morals : MonoBehaviour {

    public int level;
    public int policeMoral;
    public int correctPlug;
    public GameObject mainCam;
    public int F=0;
    // Use this for initialization
    void Start() {
        mainCam = GameObject.Find("Main Camera");
        if (level == 1)
            PlayerPrefs.SetInt("Police Contact", 0);
    }
       
	
	// Update is called once per frame
	void Update () {
        policeMoral = PlayerPrefs.GetInt("Police Contact");
        if (level == 2)
        {
            correctPlug = 1;
            mainCam.GetComponent<GameStuff>().specificCall = 3;
            mainCam.GetComponent<GameStuff>().specificBool = false;
            mainCam.GetComponent<GameStuff>().specificMessage = "Hello, I think I'm being followed... Oh shi- *gunshot*";
        }
        else if (level == 3 && policeMoral == 0)
        {
            if (F == 0)
            {
                correctPlug = 5;
                mainCam.GetComponent<GameStuff>().specificCall = 3;
                mainCam.GetComponent<GameStuff>().specificBool = false;
                mainCam.GetComponent<GameStuff>().specificMessage = "We know what you did, don't even deny it, if you value your life do not correctly connect detective Grant when he calls. Now connect me to Vito's";
                F++;
            }
            if (F == 1)
                correctPlug = 1;
            mainCam.GetComponent<GameStuff>().specificCall = 5;
            mainCam.GetComponent<GameStuff>().specificBool = false;
            mainCam.GetComponent<GameStuff>().specificMessage = "This is Dectective Grant. I must be connected immediately to the police station.";



        }
        else if (level == 3 && policeMoral == 1)
        {
            mainCam.GetComponent<GameStuff>().specificCall = 3;
            mainCam.GetComponent<GameStuff>().specificBool = false;
            mainCam.GetComponent<GameStuff>().specificMessage = "You think you're so smart huh, letting the fuzz know about our work. I hope you've said goodbye for yoour loved ones because we are coming for you";
        }
        else
        {
        }
    }

}
