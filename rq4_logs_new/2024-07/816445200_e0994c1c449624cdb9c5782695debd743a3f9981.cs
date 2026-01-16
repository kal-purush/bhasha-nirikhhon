using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class NPCManager : MonoBehaviour
{
    public static NPCManager Instance;
    private void Awake()
    {
        if (Instance == null)
        {
            Instance = this;
        }
        else
        {
            Destroy(this.gameObject);
        }
    }
    public void DisableDialogue(int option)
    {
        switch(option)
        {
            case 1:
                DialogueStats.ActiveNPC._isOption1Picked = true;
                break;
            case 2:
                DialogueStats.ActiveNPC._isOption2Picked = true;
                break;
        }
    }

    public bool IsOptionAvailable(int option)
    {
        switch(option)
        {
            case 1:
                if (DialogueStats.ActiveNPC._isOption1Picked)
                    return false;
                break;

            case 2:
                if (DialogueStats.ActiveNPC._isOption2Picked)
                    return false;
                break;
        }
        return true;
    }
}