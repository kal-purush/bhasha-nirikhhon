using UnityEngine;

public class GamePlayerController : MonoBehaviour
{
    [Header("References")]
    public Transform orientation;
    public Transform player;
    public Transform playerOBJ;
    public Rigidbody rb;
    public Animator animController;

    [Header("PlayerSetting")]
    public float walkSpeed = 0;
    public float rotationSpeed = 0;
    public float rbDrag = 3;

    // private vairables
    float horizontalInput = 0.0f;
    float verticalInput = 0.0f;
    private Vector3 moveDir = Vector3.zero;

    private void Start()
    {
        Cursor.lockState = CursorLockMode.Locked;
        Cursor.visible = false;
    }

    private void Update()
    {
        GetInput();
        PlayerRotate();
    }

    private void FixedUpdate()
    {
        PlayerMovement();
    }

    private void GetInput()
    {
        horizontalInput = Input.GetAxis("Horizontal");
        verticalInput = Input.GetAxis("Vertical");
    }

    private void PlayerRotate()
    {
        // rotate orientation
        Vector3 viewDir = player.position - new Vector3(transform.position.x, player.position.y, transform.position.z);
        orientation.forward = viewDir.normalized;

        // rotate player object
        Vector3 inputDir = orientation.forward * verticalInput + orientation.right * horizontalInput;

        if (inputDir != Vector3.zero)
        {
            // player OBJ rotate
            playerOBJ.forward = Vector3.Slerp(playerOBJ.forward, inputDir.normalized, Time.deltaTime * rotationSpeed);
        }
    }

    private void PlayerMovement()
    {
        moveDir = orientation.forward * verticalInput + orientation.right * horizontalInput;
        rb.AddForce(moveDir.normalized * walkSpeed * 10.0f, ForceMode.Force);
        rb.linearDamping = rbDrag;

        if (horizontalInput != 0 || verticalInput != 0)
        {
            animController.SetBool("IsWalk", true);
        }
        else
        {
            animController.SetBool("IsWalk", false);
        }
    }
}