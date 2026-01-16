using UnityEngine;
using UnityEngine.InputSystem; // Import the new Input System package

public class AnimationController : MonoBehaviour
{
    [SerializeField] private Animator animator;
    private PlayerInput playerInput; // Declare a variable to hold the PlayerInput component

    private void Awake()
    {
        playerInput = GetComponent<PlayerInput>(); // Get the PlayerInput component
    }

    private void Update()
    {
        Vector2 joystickLook = playerInput.actions["Look"].ReadValue<Vector2>(); // Read the "Look" action using the new Input System

        if (joystickLook.x == 0 && joystickLook.y == 0)
        {
            animator.SetBool("isMoving", false);
        }
        else
        {
            animator.SetBool("isMoving", true);
        }
    }
}