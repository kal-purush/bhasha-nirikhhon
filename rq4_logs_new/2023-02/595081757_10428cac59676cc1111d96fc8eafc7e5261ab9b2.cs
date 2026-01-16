using System;
using System.Collections.Generic;
using GameConstants;
using Movement;
using UnityEngine;
using UnityEngine.InputSystem;

public class CharacterHandler : MonoBehaviour
{
    [SerializeField] private GameObject playerInputPrefab;
    [SerializeField] private GameObject humanPrefab;
    [SerializeField] private GameObject ghostPrefab;

    [Header("Settings")] [SerializeField] private bool spawnPlayersOnStart;
    private MovementBase ghostMovementBase;
    private GameObject ghostPlayer;
    private MovementBase humanMovementBase;

    private GameObject humanPlayer;
    private InputAction inputActions;

    // ReSharper disable once ArrangeObjectCreationWhenTypeEvident
    private List<InputDevice> inputDevices = new List<InputDevice>();

    private PlayerInput player1Input;
    private PlayerInput player2Input;

    private void Update()
    {
        if (Input.GetKeyDown(KeyCode.P))
        {
            SpawnPlayerInput();
            SpawnCharacters();
        }

        if (Input.GetKey(KeyCode.LeftControl) && Input.GetKeyDown(KeyCode.R))
        {
            inputDevices = GetAllInputDevices();
        }
    }

    private void OnHumanMovementInput(InputAction.CallbackContext context)
    {
        var newMovementInput = context.ReadValue<Vector2>();
        humanMovementBase.MovementInput.z = newMovementInput.y;
        humanMovementBase.MovementInput.x = newMovementInput.x;
    }

    private void OnHumanNoMovementInput(InputAction.CallbackContext obj)
    {
        humanPlayer.GetComponent<MovementBase>().MovementInput = Vector3.zero;
    }

    private void OnGhostMovementInput(InputAction.CallbackContext context)
    {
        var newMovementInput = context.ReadValue<Vector2>();
        ghostMovementBase.MovementInput.z = newMovementInput.y;
        ghostMovementBase.MovementInput.x = newMovementInput.x;
    }

    private void OnGhostNoMovementInput(InputAction.CallbackContext obj)
    {
        ghostMovementBase.MovementInput = Vector3.zero;
    }

    private void OnGhostJumpPressed(InputAction.CallbackContext context)
    {
        ghostMovementBase.shouldJump = true;
    }

    private void OnGhostJumpReleased(InputAction.CallbackContext obj)
    {
        ghostMovementBase.shouldJump = false;
    }

    private void OnHumanJumpPressed(InputAction.CallbackContext obj)
    {
        humanMovementBase.shouldJump = true;
    }

    private void OnHumanJumpReleased(InputAction.CallbackContext obj)
    {
        humanMovementBase.shouldJump = false;
    }

    private void SpawnCharacters()
    {
        humanPlayer = Instantiate(humanPrefab);
        ghostPlayer = Instantiate(ghostPrefab);

        humanMovementBase = humanPlayer.GetComponent<MovementBase>();
        ghostMovementBase = ghostPlayer.GetComponent<MovementBase>();

        foreach (var action in player1Input.currentActionMap.actions)
        {
            switch (action.name)
            {
                case Strings.Move:
                    action.performed += OnHumanMovementInput;
                    action.canceled += OnHumanNoMovementInput;
                    break;
                case Strings.Jump:
                    action.started += OnHumanJumpPressed;
                    action.canceled += OnHumanJumpReleased;
                    break;
            }
        }

        if (player2Input == null)
        {
            return;
        }

        foreach (var action in player2Input.currentActionMap.actions)
        {
            switch (action.name)
            {
                case Strings.Move:
                    action.performed += OnGhostMovementInput;
                    action.canceled += OnGhostNoMovementInput;
                    break;
                case Strings.Jump:
                    action.started += OnGhostJumpPressed;
                    action.canceled += OnGhostJumpReleased;
                    break;
            }
        }
    }

    private void OnDisable()
    {
        if (player1Input != null)
        {
            foreach (var action in player1Input.currentActionMap.actions)
            {
                Debug.Log($"Unsubscribed {action}.");
                switch (action.name)
                {
                    case Strings.Move:
                        action.performed -= OnHumanMovementInput;
                        action.canceled -= OnHumanNoMovementInput;
                        break;
                    case Strings.Jump:
                        action.started -= OnHumanJumpPressed;
                        action.canceled -= OnHumanJumpReleased;
                        break;
                }
            }
        }

        if (player2Input == null)
        {
            return;
        }

        {
            foreach (var action in player2Input.currentActionMap.actions)
            {
                Debug.Log($"Unsubscribed {action}.");
                switch (action.name)
                {
                    case Strings.Move:
                        action.performed -= OnGhostMovementInput;
                        action.canceled -= OnGhostNoMovementInput;
                        break;
                    case Strings.Jump:
                        action.started -= OnGhostJumpPressed;
                        action.canceled -= OnGhostJumpReleased;
                        break;
                }
            }
        }
    }


    private void SpawnPlayerInput()
    {
        foreach (var inputDevice in inputDevices)
        {
            if (player1Input == null)
            {
                player1Input = Game.PlayerInputManager.JoinPlayer(0, -1, null, inputDevice);
            }
            else if (player2Input == null)
            {
                player2Input = Game.PlayerInputManager.JoinPlayer(1, -1, null, inputDevice);
            }
        }
    }

    private List<InputDevice> GetAllInputDevices()
    {
        var devices = new List<InputDevice> { Keyboard.current };
        devices.AddRange(Gamepad.all);
        Debug.Log($"Fetched {devices.Count} device(s)");
        foreach (var inputDevice in devices)
        {
            Debug.Log($"{inputDevice}");
        }

        return devices;
    }

    public void OnPlayerJoined(PlayerInput playerInput)
    {
        if (player1Input == null)
        {
            player1Input = playerInput;
        }

        else if (player2Input == null)
        {
            player2Input = playerInput;
        }
    }
}