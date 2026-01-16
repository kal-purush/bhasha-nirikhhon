using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class ControlTypeIsActive : MonoBehaviour
{
    private Outline _outline;
    [SerializeField] private string _controlTypeName;
    public PlayerControlTypeHolder.ControlTypeEnum currentControlType;

    private void Update()
    {
        currentControlType = PlayerControlTypeHolder.ControlType;

        _outline = GetComponent<Outline>();

        if (currentControlType == (PlayerControlTypeHolder.ControlTypeEnum)Enum.Parse(typeof(PlayerControlTypeHolder.ControlTypeEnum), _controlTypeName))
        {
            _outline.enabled = true;
        }
        else
        {
            _outline.enabled = false;
        }
    }
}