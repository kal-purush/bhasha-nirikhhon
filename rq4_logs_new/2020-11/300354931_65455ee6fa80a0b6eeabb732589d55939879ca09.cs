using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class RetreatConfirmation : MonoBehaviour
{

    private EventController controller;

    public Button retreat;

    void Start()
    {
        controller = GameObject.Find("EventController").GetComponent<EventController>();
        retreat.onClick.AddListener(() => controller.DisplayPlayerLose());
    }    

}