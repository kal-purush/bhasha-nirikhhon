using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class MovementScript : MonoBehaviour
{
    public float speed = 200f;
    public Rigidbody2D rb;
    public SpriteRenderer sr;
    private Vector2 direction;

    public Animator anim;
    private string currentState;
    const string _IDLE = "Player_Sit";
    const string _RUN = "Player_Run";
    const string _FIRE = "Player_Smash";

    // Start is called before the first frame update
    void Start()
    {
        rb = GetComponent<Rigidbody2D>();
        sr = GameObject.FindGameObjectWithTag("PlayerSprite").GetComponent<SpriteRenderer>();
        anim = GetComponent<Animator>();
        rb.freezeRotation = true;
    }

    // Update is called once per frame
    void Update()
    {
        direction = new Vector2(Input.GetAxis("Horizontal"), Input.GetAxis("Vertical"));
    }

    void FixedUpdate()
    {
       // Movement
        rb.velocity = new Vector2(direction.x * speed * Time.fixedDeltaTime, rb.velocity.y * speed * Time.fixedDeltaTime);
        Debug.Log(rb.velocity);

        if (direction.x != 0)
        {
            changeAnimationState(_RUN);
        }
        else
        {
            changeAnimationState(_IDLE);
        }

        flip();
    }


    void flip()
    {
        if (rb.velocity.x > 0)
        {
            sr.flipX = false;
        }
        else if (rb.velocity.x < 0)
        {
            sr.flipX = true;
        }
    }

    private void changeAnimationState(string newState)
    {
        if (currentState == newState) return;

        anim.Play(newState);

        currentState = newState;
    }

    private bool isAnimationPlaying(string stateName)
    {
        return anim.GetCurrentAnimatorStateInfo(0).IsName(stateName) && anim.GetCurrentAnimatorStateInfo(0).normalizedTime < 1.0f;
    }
}