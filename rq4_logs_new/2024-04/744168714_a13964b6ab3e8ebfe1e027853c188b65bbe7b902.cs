using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class AssistMode : MonoBehaviour
{

    public GameObject player;
    public float assistModeHealthRegen;
    public float regularHealthRegen;

    public void ToggleAssistMode(bool mode)
    {
        if (mode)
        {
            player.GetComponent<Controller>().healthIncreaseRate = assistModeHealthRegen;
        }
        else
        {
            player.GetComponent<Controller>().healthIncreaseRate = regularHealthRegen;
        }
    }
}