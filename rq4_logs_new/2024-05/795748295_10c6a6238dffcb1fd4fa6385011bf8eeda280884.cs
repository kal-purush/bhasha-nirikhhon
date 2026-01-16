using System.Collections;
using System.Collections.Generic;
using Unity.VisualScripting;
using UnityEngine;

public class AnimationScript : MonoBehaviour
{
    [Header("Animation Script")]
    [SerializeField] public Rigidbody2D rb;
    [SerializeField] public Animator anim;
    [SerializeField] public SpriteRenderer sr;
    private string currentState;
    private bool isAttacking = false;
    private bool isIdle = true;
    private KeyCode lastKey { get; set; }
    const string _IDLEU = "Player_Idle_Up";
    const string _RUNU = "Player_Run_Up";
    const string _IDLED = "Player_Idle_Down";
    const string _RUND = "Player_Run_Down";
    const string _IDLES = "Player_Idle_Side";
    const string _RUNS = "Player_Run_Side";
    const string _ATTACKS = "Player_Attack_Side";
    const string _ATTACKU = "Player_Attack_Up";
    const string _ATTACKD = "Player_Attack_Down";

    // Update is called once per frame
    void Update()
    {
        Flip();
    }

    void FixedUpdate()
    {
        if (isAnimationPlaying(_ATTACKD) || isAnimationPlaying(_ATTACKS) || isAnimationPlaying(_ATTACKU) || Input.GetKeyDown(KeyCode.Space))
        {
            isAttacking = true;
            changeAnimationState(_ATTACKS);
            return;
        }
        else
        {
            isAttacking = false;
            if (Input.GetKey(KeyCode.W))
            {
                changeAnimationState(_RUNU);
                lastKey = KeyCode.W;
            }
            else if (Input.GetKey(KeyCode.S))
            {
                changeAnimationState(_RUND);
                lastKey = KeyCode.S;
            }
            else if (Input.GetKey(KeyCode.A) || Input.GetKey(KeyCode.D))
            {
                changeAnimationState(_RUNS);
                lastKey = KeyCode.D;
            }
            else
            {
                if (lastKey == KeyCode.W)
                {
                    changeAnimationState(_IDLEU);
                }
                else if (lastKey == KeyCode.S)
                {
                    changeAnimationState(_IDLED);
                }
                else if (lastKey == KeyCode.D)
                {
                    changeAnimationState(_IDLES);
                }

            }
        }
    }

    void changeAnimationState(string newState)
    {
        if (currentState == newState) return;

        anim.Play(newState);

        currentState = newState;
    }

    private bool isAnimationPlaying(string stateName)
    {
        return anim.GetCurrentAnimatorStateInfo(0).IsName(stateName) && anim.GetCurrentAnimatorStateInfo(0).normalizedTime < 1.0f;
    }


    void Flip()
    {
        if (rb.velocity.x > 0)
        {
            sr.flipX = true;
        }
        else if (rb.velocity.x < 0)
        {
            sr.flipX = false;
        }
    }
}