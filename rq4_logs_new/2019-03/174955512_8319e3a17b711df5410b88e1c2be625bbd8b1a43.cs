using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class TimerScript : MonoBehaviour
{
    const int InitTimeSeconds = 100;
    [SerializeField]
    Text intText;
    [SerializeField]
    Text floatText;
    public bool Started {get; set;}
    int leftFrame;
    int leftSeconds;
    int leftMilsec;

    // Start is called before the first frame update
    void Start()
    {
        Started = true;
        leftFrame = 60 * InitTimeSeconds;
    }

    private void UpdateTimeDisplay(){
        string GetTimeDisp(int milsec){
            if (milsec < 10) return $".00{milsec}";
            if (milsec < 100) return $".0{milsec}";
            return $".{milsec}";
        }
        leftSeconds = leftFrame / 60;
        leftMilsec = (int)((leftFrame % 60) * (1.0 / 60) * 1000);
        intText.text = leftSeconds.ToString();
        floatText.text = GetTimeDisp(leftMilsec);
    }

    // Update is called once per frame
    void Update()
    {
        void UpdateTime(){
            if(!Started) return;
            leftFrame--;
            UpdateTimeDisplay();
        }
        UpdateTime();
    }
}