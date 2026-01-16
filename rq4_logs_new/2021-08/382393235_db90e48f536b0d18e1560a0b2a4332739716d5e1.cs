using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ButtonAvailable : MonoBehaviour
{
    // Start is called before the first frame update
    private GameObject button;
    void Start()
    {
        button = GameObject.Find("CokeLevelButton");
    }

    // Update is called once per frame
    void Update()
    {
        if(Data.money>=Data.CokeCost){
            button.SetActive (true);
        }else{
            button.SetActive (false);
        }
    }
}