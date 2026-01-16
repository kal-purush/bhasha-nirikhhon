using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class SettingController : MonoBehaviour
{
    public Toggle mute;
    public Text score;
    // Start is called before the first frame update
    void Awake()
    {
        mute.isOn = MainController.mute;
        Record.LoadScore();
        if (Record.score < 0)
        {
            score.text = "还没有记录哦";
        } else
        {
            score.text = "" + Record.score;
        }
    }

    public void ChangeMute()
    {
        MainController.mute = mute.isOn;
        Record.SaveMute();
    }
}