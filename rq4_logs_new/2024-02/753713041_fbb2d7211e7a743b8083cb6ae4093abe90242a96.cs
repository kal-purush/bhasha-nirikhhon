using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using TMPro;

public class UIManager : MonoBehaviour
{
    public TextMeshProUGUI statusText; 

    private void Start()
    {
        statusText.text = "Component Status:";
    }

    public void UpdateComponentStatus(bool? isAttached)
    {
        if (isAttached.HasValue)
        {
            if (isAttached.Value)
            {
                statusText.text = "Component Status: <color=#00FF00>Attached</color>";
            }

            else
            {
                statusText.text = "Component Status: <color=#FF0000>Detached</color>";
            }
        }

        else
        {
            statusText.text = "Component Status:";
        }
    }

    private void OnEnable()
    {
        ComponentManager.OnComponentStatusChanged += UpdateComponentStatus;
    }

    private void OnDisable()
    {
        ComponentManager.OnComponentStatusChanged -= UpdateComponentStatus;
    }
}