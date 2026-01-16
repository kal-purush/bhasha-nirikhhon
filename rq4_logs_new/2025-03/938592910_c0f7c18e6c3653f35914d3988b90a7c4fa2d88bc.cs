using UnityEngine;
using UnityEngine.InputSystem;

[RequireComponent(typeof(PlayerInput))]
public class CameraController : MonoBehaviour
{
    [Header("Camera References")]
    [SerializeField] private Transform cameraHolder;
    [SerializeField] private Transform playerBody;

    [Header("Input Actions")]
    [SerializeField] private InputActionReference lookAction;

    [Header("Look Settings")]
    [SerializeField] private float mouseSensitivity = 20f;
    [SerializeField] private bool invertY = false;
    [SerializeField] private bool invertX = false;
    [SerializeField] private float smoothTime = 0.05f;
    [SerializeField] private float maxLookUpAngle = 80f;
    [SerializeField] private float maxLookDownAngle = -80f;

    // Current rotation values
    private float xRotation = 0f;
    private float yRotation = 0f;

    // Target rotation values (for smoothing)
    private float targetXRotation = 0f;
    private float targetYRotation = 0f;

    // Smoothing velocity storage
    private float xVelocity = 0f;
    private float yVelocity = 0f;

    // Input value
    private Vector2 lookInput;

    // Player input component reference
    private PlayerInput playerInput;

    private void Awake()
    {
        // Get PlayerInput component
        playerInput = GetComponent<PlayerInput>();

        // If no camera holder assigned, try to find one
        if (cameraHolder == null)
        {
            cameraHolder = transform.Find("CameraHolder");

            // If it's still null, try to find the main camera's parent
            if (cameraHolder == null && Camera.main != null)
            {
                cameraHolder = Camera.main.transform.parent;
            }
        }

        // If no player body assigned, use this transform
        if (playerBody == null)
        {
            playerBody = transform;
        }

        // Lock cursor to game window and hide it
        Cursor.lockState = CursorLockMode.Locked;
        Cursor.visible = false;
    }

    private void OnEnable()
    {
        // Enable look action and subscribe to events
        if (lookAction != null)
        {
            lookAction.action.Enable();
            lookAction.action.performed += OnLook;
            lookAction.action.canceled += OnLook;
        }
    }

    private void OnDisable()
    {
        // Disable look action and unsubscribe from events
        if (lookAction != null)
        {
            lookAction.action.Disable();
            lookAction.action.performed -= OnLook;
            lookAction.action.canceled -= OnLook;
        }
    }

    private void OnLook(InputAction.CallbackContext context)
    {
        // Read the mouse input value
        lookInput = context.ReadValue<Vector2>();
    }

    private void Update()
    {
        HandleMouseLook();
    }

    private void HandleMouseLook()
    {
        // Calculate rotation adjustment based on input and sensitivity
        float mouseX = lookInput.x * mouseSensitivity * Time.deltaTime;
        float mouseY = lookInput.y * mouseSensitivity * Time.deltaTime;

        // Apply inversion if enabled
        if (invertX) mouseX = -mouseX;
        if (invertY) mouseY = -mouseY;

        // Update target rotations
        targetXRotation -= mouseY; // Negative to invert the rotation (moving mouse up rotates camera up)
        targetYRotation += mouseX;

        // Clamp vertical rotation
        targetXRotation = Mathf.Clamp(targetXRotation, maxLookDownAngle, maxLookUpAngle);

        // Apply smoothing
        if (smoothTime > 0)
        {
            xRotation = Mathf.SmoothDamp(xRotation, targetXRotation, ref xVelocity, smoothTime);
            yRotation = Mathf.SmoothDamp(yRotation, targetYRotation, ref yVelocity, smoothTime);
        }
        else
        {
            xRotation = targetXRotation;
            yRotation = targetYRotation;
        }

        // Apply rotations
        cameraHolder.localRotation = Quaternion.Euler(xRotation, 0f, 0f);
        playerBody.rotation = Quaternion.Euler(0f, yRotation, 0f);
    }

    // Public methods
    /// Temporarily unlocks the cursor, useful for UI interactions
    public void UnlockCursor()
    {
        Cursor.lockState = CursorLockMode.None;
        Cursor.visible = true;
    }

    /// Re-locks the cursor, call this when returning to gameplay
    public void LockCursor()
    {
        Cursor.lockState = CursorLockMode.Locked;
        Cursor.visible = false;
    }

    /// Sets the mouse sensitivity
    public void SetSensitivity(float newSensitivity)
    {
        mouseSensitivity = newSensitivity;
    }

    /// Resets the camera to forward position
    public void ResetCameraRotation()
    {
        targetXRotation = 0f;
        targetYRotation = playerBody.eulerAngles.y;
    }

    /// Rotates the camera to look at a specific world position
    public void LookAt(Vector3 worldPosition)
    {
        // Calculate direction vector
        Vector3 direction = worldPosition - cameraHolder.position;

        // Calculate the rotation to look at the target
        Quaternion lookRotation = Quaternion.LookRotation(direction);

        // Extract Euler angles
        Vector3 eulerRotation = lookRotation.eulerAngles;

        // Update target rotations (with proper clamping for X)
        targetYRotation = eulerRotation.y;

        // Need to handle the x rotation differently due to clamping
        float xAngle = eulerRotation.x;
        if (xAngle > 180) xAngle -= 360; // Convert to -180 to 180 range

        targetXRotation = Mathf.Clamp(xAngle, maxLookDownAngle, maxLookUpAngle);
    }
}