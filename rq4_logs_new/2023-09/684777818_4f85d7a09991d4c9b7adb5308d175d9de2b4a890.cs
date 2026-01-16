using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.InputSystem;

public class FrogInputSystem : MonoBehaviour
{
    private Rigidbody rb;
    private PlayerInput playerInput;
    private bool isGrounded;
    private ConstantForce gravity;

    public InputAction playerControls;

    public float jumpHeight;
    public float maxJumpHeight;
    public float rotationSpeed;

    Vector2 rotateDirection = Vector2.zero;

    private void OnEnable()
    {
        playerControls.Enable();
    }

    private void OnDisable()
    {
        playerControls.Disable();
    }

    // Start is called before the first frame update
    void Start()
    {
        rb = GetComponent<Rigidbody>();
        playerInput = GetComponent<PlayerInput>();

        PlayerInputActions playerInputActions = new PlayerInputActions();
        playerInputActions.Player.Enable();
        playerInputActions.Player.Jump.canceled += Jump;
        //playerInputActions.Player.Rotate.performed += Rotate;

        gravity = gameObject.AddComponent<ConstantForce>();
        updateGravity(new Vector3(0, -1.0f, 0));

        isGrounded = true;
    }

    private void Update()
    {
        rotateDirection = playerControls.ReadValue<Vector2>();
    }

    void FixedUpdate()
    {
        transform.Rotate(0f, rotateDirection.x * rotationSpeed, rotateDirection.y * rotationSpeed);
    }

    public void Jump(InputAction.CallbackContext context)
    {
        float calculatedJump;

        if (context.canceled && isGrounded)
        {
            isGrounded = false;

            // free rigid body constraints
            rb.constraints = RigidbodyConstraints.FreezeRotation;

            calculatedJump = (float)context.duration * jumpHeight;

            if (calculatedJump > maxJumpHeight)
            {
                calculatedJump = maxJumpHeight;
            }

            rb.AddForce(calculatedJump * (transform.forward + transform.up), ForceMode.Impulse);
        }
    }

    public void Rotate(InputAction.CallbackContext context)
    {
        
    }

    private void OnCollisionEnter(Collision collision)
    {
        isGrounded = true;

        if (LayerMask.LayerToName(collision.gameObject.layer) == "Wall")
        {
            stickToWall(collision.contacts[0]);
        }
    }

    private void stickToWall(ContactPoint contact)
    {
        rb.velocity = Vector3.zero;
        rb.constraints = RigidbodyConstraints.FreezeAll;

        var rot = Quaternion.FromToRotation(-transform.up, contact.normal);

        updateGravity(-contact.normal);

        transform.rotation *= rot;
    }

    private void updateGravity(Vector3 newGravity)
    {
        gravity.force = 9.81f * newGravity;
    }
}