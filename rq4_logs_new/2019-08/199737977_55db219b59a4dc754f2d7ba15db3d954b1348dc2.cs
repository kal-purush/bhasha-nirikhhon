using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlayerAnimatorScript : MonoBehaviour
{
    public GameObject animatorParent;
    public PlayerScript playerScript;
    public Animator animator;

    void Awake()
    {
        playerScript = animatorParent.GetComponent<PlayerScript>();
        animator = GetComponent<Animator>();
    }

    public void IdleActivateEndFunc()
    {
        animator.SetTrigger("TriggerIdle");
        playerScript.currentState = PlayerScript.playerStates.idle;
        playerScript.isAnimationTriggered = false;
    }

    public void IdleDeactivateEndFunc()
    {

    }

    public void RunActivateEndFunc()
    {

    }

    public void RunDeactivateEndFunc()
    {

    }

    public void TriggerStartIdle()
    {
        animator.SetTrigger("TriggerStartIdle");
    }
}