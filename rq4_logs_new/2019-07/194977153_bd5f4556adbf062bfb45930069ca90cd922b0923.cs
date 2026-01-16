using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class InputController : MonoBehaviour
{
    [HideInInspector] public bool isJumping, isSliding;
    [SerializeField] private GameObject buttonControls;

    [Tooltip("Use touch controls")]
    [SerializeField] private bool touchControls;

    private void Awake()
    {
        if (SystemInfo.deviceType == DeviceType.Handheld)
        {
            EnableTouchScreenInput();
        }
    }

    private void Update()
    {
        if (!touchControls)
        {
            ApplyKeyboardInputs();
        }
    }

    private void EnableTouchScreenInput()
    {
        buttonControls.SetActive(true);
        touchControls = true;
    }

    private void ApplyKeyboardInputs()
    {
        isJumping = Input.GetKeyDown(KeyCode.W);
        isSliding = Input.GetKey(KeyCode.E);
    }
}