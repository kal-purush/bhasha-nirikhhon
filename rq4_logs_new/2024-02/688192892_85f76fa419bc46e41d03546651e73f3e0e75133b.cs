using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using OmnicatLabs.CharacterControllers;
using UnityEngine.InputSystem;

public class WeaponSwap : MonoBehaviour
{
    public List<GameObject> weapons;

    private OmnicatLabs.CharacterControllers.CharacterController player;
    private Vector2 scrollInput;

    private void Start()
    {
        player = OmnicatLabs.CharacterControllers.CharacterController.Instance;
    }

    public void OnSwap(InputAction.CallbackContext ctx)
    {
        scrollInput = ctx.ReadValue<Vector2>();
    }

    private void Update()
    {
        Debug.Log(scrollInput);
    }
}