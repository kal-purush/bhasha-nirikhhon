using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Formation
{
    public enum Formation_ : int
    {
        forward,
        forward_right,
        right,
        behind_right,
        behind,
        behind_left,
        left,
        forward_left,
        FORMATION_COUNT
    }
    public Formation_ value;

    public static Formation operator ++(Formation value)
    {
        switch (value.value)
        {
            case Formation_.forward:
                value.value = Formation_.forward_right;
                break;
            case Formation_.forward_right:
                value.value = Formation_.right;
                break;
            case Formation_.right:
                value.value = Formation_.behind_right;
                break;
            case Formation_.behind_right:
                value.value = Formation_.behind;
                break;
            case Formation_.behind:
                value.value = Formation_.behind_left;
                break;
            case Formation_.behind_left:
                value.value = Formation_.left;
                break;
            case Formation_.left:
                value.value = Formation_.forward_left;
                break;
            case Formation_.forward_left:
                value.value = Formation_.forward;
                break;
            default:
                DebugLogger.Log("Enum error!");
                break;
        }

        return value;
    }

    public static Formation operator --(Formation value)
    {
        switch (value.value)
        {
            case Formation_.forward:
                value.value = Formation_.forward_left;
                break;
            case Formation_.forward_right:
                value.value = Formation_.forward;
                break;
            case Formation_.right:
                value.value = Formation_.forward_right;
                break;
            case Formation_.behind_right:
                value.value = Formation_.right;
                break;
            case Formation_.behind:
                value.value = Formation_.behind_right;
                break;
            case Formation_.behind_left:
                value.value = Formation_.behind;
                break;
            case Formation_.left:
                value.value = Formation_.behind_left;
                break;
            case Formation_.forward_left:
                value.value = Formation_.left;
                break;
            default:
                DebugLogger.Log("Enum error!");
                break;
        }

        return value;
    }
}