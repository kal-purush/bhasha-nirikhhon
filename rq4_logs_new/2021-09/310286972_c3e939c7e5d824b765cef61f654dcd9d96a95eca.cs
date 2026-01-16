using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class MobileOption_Howto : MonoBehaviour
{
    public GameObject howPC, howMo;
    public bool mobileMode;
    private new CriAtomSource audio;
    void Start()
    {
        audio = (CriAtomSource)GetComponent("CriAtomSource");
    }

    public void OnClick()
    {
        audio.Play("System_Decision");
        if (!mobileMode)
        {
            howPC.SetActive(true);
        }
        else
        {
            howMo.SetActive(true);
        }
    }
}