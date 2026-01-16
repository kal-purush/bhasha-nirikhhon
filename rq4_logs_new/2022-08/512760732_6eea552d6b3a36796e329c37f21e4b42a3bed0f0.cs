using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class ShowPrompt : MonoBehaviour
{

    public Canvas EPromptCanvas;

    
    void OnTriggerEnter(Collider ElTrigger)
    {
        if(ElTrigger.tag == "Player")
        {
            Debug.Log("Estas cerca del NPC");
            EPromptCanvas.enabled = true;
        }
    }
    void OnTriggerExit(Collider SalirTrigger)
    {
        Debug.Log("Estas lejos del NPC");
        EPromptCanvas.enabled = false;
    }
}