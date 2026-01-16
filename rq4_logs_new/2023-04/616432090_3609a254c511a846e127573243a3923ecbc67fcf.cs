using UnityEngine;
using UnityEngine.InputSystem;

public class Player : MonoBehaviour
{
    [Header("Movement")]
    [SerializeField] private float Speed = 5.0f;
    [SerializeField] private float JumpForce = 1.5f;

    [Header("Grabbing")]

    [SerializeField] private float MaxGrabWeight = 50.0f;

    [Header("Animation")]
    [SerializeField] private Animator PlayerAnimator;

    private Vector2 Movement;

    private Rigidbody RB;

    private void Awake()
    {
        RB = GetComponent<Rigidbody>();
    }
    public void OnMove(InputAction.CallbackContext context)
    {
        Movement = context.ReadValue<Vector2>();
    }

    private void Update()
    {
        //Gravity();

        if (Movement.x >= 0.1f || Movement.y >= 0.1f || Movement.x <= -0.1f || Movement.y <= -0.1f)
        {
            PlayerAnimator.SetBool("IsRunning", true);
            MovePlayer();
        }
        else
        {
            PlayerAnimator.SetBool("IsRunning", false);
        }

    }
    private void MovePlayer()
    {
        Vector3 move = new Vector3(Movement.x, RB.velocity.y, Movement.y);
        transform.LookAt(move + transform.position);
        RB.MovePosition(transform.position + move * Speed * Time.deltaTime);
    }
}