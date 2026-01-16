using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class KeyboardScript : MonoBehaviour
{

    public InputField TextField;
    public GameObject EngLayoutSml;

    public void alphabetFunction(string alphabet)
    {


        TextField.text=TextField.text + alphabet;

    }

    public void BackSpace()
    {

        if(TextField.text.Length>0) TextField.text= TextField.text.Remove(TextField.text.Length-1);

    }

    public void CloseAllLayouts()
    {

     
        EngLayoutSml.SetActive(false);


    }

    public void ShowLayout(GameObject SetLayout)
    {

        CloseAllLayouts();
        SetLayout.SetActive(true);

    }

}