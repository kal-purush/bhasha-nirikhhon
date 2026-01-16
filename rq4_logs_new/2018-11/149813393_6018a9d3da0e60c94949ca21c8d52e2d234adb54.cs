using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using Fungus;

public class MusicController : MonoBehaviour {

    public AudioSource clip1LowHope;
    public AudioSource clip2MidHope;
    public AudioSource clip3HighHope;
    public Flowchart myFlowchart;
    public float fadeSpeed;


    // Use this for initialization
	void Start () {
		
	}
	
	// Update is called once per frame
	void Update () {
        if(myFlowchart.GetFloatVariable("mentalStateScale") < gameObject.GetComponent<MentalStateManagement>().mentalStateLowThreshold){
            //adjust volume so only low hope music is playing
           
            //FADE IN
            if (clip1LowHope.volume < 1)
            {
                clip1LowHope.volume += fadeSpeed * Time.deltaTime;
            }


         
            //fade out
            if (clip2MidHope.volume > 0)
            {
                clip2MidHope.volume -= fadeSpeed * Time.deltaTime;
            }

    
            //fade out
            if (clip3HighHope.volume > 0)
            {
                clip3HighHope.volume -= fadeSpeed * Time.deltaTime;
            }

        }
        else if (myFlowchart.GetFloatVariable("mentalStateScale") > gameObject.GetComponent<MentalStateManagement>().mentalStateHiThreshold){
            //adjust volume so only high hope music is playing

          
            //fade out
            if (clip1LowHope.volume > 0)
            {
                clip1LowHope.volume -= fadeSpeed * Time.deltaTime;
            }

            //fade out
            if (clip2MidHope.volume > 0)
            {
                clip2MidHope.volume -= fadeSpeed * Time.deltaTime;
            }


            //fade in
            if (clip3HighHope.volume < 1)
            {
                clip3HighHope.volume += fadeSpeed * Time.deltaTime;
            }
        }
        else{
            //adjust volume so only middle state music is playing

            //fade out
            if (clip1LowHope.volume > 0)
            {
                clip1LowHope.volume -= fadeSpeed * Time.deltaTime;
            }


            //fade in
            if (clip2MidHope.volume < 1)
            {
                clip2MidHope.volume += fadeSpeed * Time.deltaTime;
            }

            //fade out
            if (clip3HighHope.volume > 0)
            {
                clip3HighHope.volume -= fadeSpeed * Time.deltaTime;
            }
        }
	}
}