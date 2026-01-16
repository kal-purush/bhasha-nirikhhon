using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class StartScene_Mobile_Howto : MonoBehaviour
{
    public GameObject pcHow, mobileHow, start;
    public bool mobileMode;
    private new CriAtomSource audio;

    void Start()
    {
        audio = (CriAtomSource)GetComponent("CriAtomSource");
    }

    // Update is called once per frame
    void Update()
    {
        
    }

    public void OnClick()
    {
        if (!mobileMode)
        {
            audio.Play("System_Decision");
            start.SetActive(false);
            pcHow.SetActive(true);
        }
        else
        {
            audio.Play("System_Decision");
            start.SetActive(false);
            mobileHow.SetActive(true);
        }
    }
}