using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.InputSystem;

public class Marcos : MonoBehaviour
{
    public Rigidbody body;
    Vector2 moveDirection = Vector2.zero;

    public float speed = 5;
    public Move playerControls;
    public float jumpForce = 40f;
    public bool isGrounded;
    public bool moving = false;
    public bool isRandom = false;

    private InputAction move;
    private InputAction jump;
    // Start is called before the first frame update

    private void Awake()
    {
        playerControls = new Move();

        move = playerControls.Shmoove.Move;
        if (!isRandom)
        {
            move.started += ctx => moving = true;
            move.canceled += ctx => moving = false;
        }
    }
    //void OnCollisionStay()
    //{
    //  isGrounded = true;
    // }
    private void OnCollisionStay(Collision collision)
    {
        if (collision.gameObject.tag == "Ground")
        {
            isGrounded = true;
        }
    }

    void Start()
    {
        if (isRandom)
            StartCoroutine(RandomMovement());
    }

    // Update is called once per frame
    void Update()
    {
        if (!isRandom)
        moveDirection = move.ReadValue<Vector2>();
        
    }

    private void FixedUpdate()
    {
        if (moving && isGrounded)
        {
            body.velocity = new Vector3(moveDirection.x * speed, 0, moveDirection.y * speed);

            float angle = Vector3.SignedAngle(Vector3.left, body.velocity.normalized, Vector3.up);
            transform.rotation = Quaternion.Euler(0, angle, 0);

            body.AddForce(Vector3.up * jumpForce, ForceMode.Impulse);
            isGrounded = false;
        }
    }

    private void OnEnable()
    {

        move.Enable();
    }

    private void OnDisable()
    {
        move.Disable();
    }

    private void Jump(InputAction.CallbackContext context)
    {

    }

    IEnumerator RandomMovement()
    {
        moving = true;
        while (true)
        {
            moveDirection = new Vector2(Random.Range(-1f, 1f), Random.Range(-1f, 1f)).normalized;
            yield return new WaitForSeconds(Random.Range(1f, 5f));
        }
    }
}