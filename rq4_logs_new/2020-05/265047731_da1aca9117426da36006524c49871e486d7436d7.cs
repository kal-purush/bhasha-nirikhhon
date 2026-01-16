using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class FallBehaviour : StateMachineBehaviour
{
    AudioSource myAudioSource;
    SFXPlayer sfxPlayer;
    BoxCollider2D myFeet;

    // OnStateEnter is called when a transition starts and the state machine starts to evaluate this state
    override public void OnStateEnter(Animator animator, AnimatorStateInfo stateInfo, int layerIndex)
    {
        myAudioSource = animator.GetComponent<AudioSource>();
        sfxPlayer = FindObjectOfType<SFXPlayer>();
        myFeet = animator.GetComponent<BoxCollider2D>();
    }

    // OnStateExit is called when a transition ends and the state machine finishes evaluating this state
    override public void OnStateExit(Animator animator, AnimatorStateInfo stateInfo, int layerIndex)
    {
        bool playerIsTouchingGround = myFeet.IsTouchingLayers(LayerMask.GetMask("Ground"));
        if (playerIsTouchingGround)
        {
            myAudioSource.PlayOneShot(sfxPlayer.GetLandingClip(), sfxPlayer.GetLandingVolume());
        }
    }
}