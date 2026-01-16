using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using UnityEngine.Events;

//Script for controlling levels, scores, etc. 
//Has the UI as a child object

public class GameManager : MonoBehaviour
{

    private UnityAction submitAction;
    Button submitButton;

    // Start is called before the first frame update
    void Start()
    {
        submitButton = GetComponentInChildren<Button>();
        submitAction += Submit;
        submitButton.onClick.AddListener(submitAction);
    }

    // Update is called once per frame
    void Update()
    {
        
    }

    void Submit()
    {
        Debug.Log("Button Pressed");
    }
    
}