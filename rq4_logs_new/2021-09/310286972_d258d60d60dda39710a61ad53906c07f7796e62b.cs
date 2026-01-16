using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class Prologue_Lan_M : MonoBehaviour
{
    public Text skip, yes, no;
    public string language;//PlayrPrefsで判別
    void Start()
    {
        language = PlayerPrefs.GetString("language");
        if (language == "Japanese")
        {
            return;
        }
        else if (language == "English")
        {
            EnglishSkip();
        }
    }

    void EnglishSkip()
    {
        skip.text = "Do  you  skip  this  movie？";
        yes.text = "Yes";
        no.text = "No";
    }
}