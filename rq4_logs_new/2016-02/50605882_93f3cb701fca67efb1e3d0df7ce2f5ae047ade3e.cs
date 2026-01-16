using UnityEngine;
using System.Collections;

public class JumpPlatforms : MonoBehaviour {
    //CAMPOS
    public float JumpMagnitude = 20;

    //METODOS
    public void ControllerEnter2D (CharacterController2D controller)
    {
        controller.SetVerticalForce(JumpMagnitude);
        Debug.LogWarning("no rula");
    }
}