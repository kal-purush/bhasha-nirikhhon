using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class TransitionBehaviour : StateMachineBehaviour
{
    [SerializeField] string animation;
    Animator transitionAnimator;
    override public void OnStateEnter(Animator animator, AnimatorStateInfo stateInfo, int layerIndex)
    {
        transitionAnimator = animator;
        BasicCharacter.Instance.canReceiveInput = true;
        BasicCharacter.Instance.onAttack += OnAttack;
    }
    public void OnAttack()
    {
        transitionAnimator.SetTrigger(animation);
    }
    override public void OnStateExit(Animator animator, AnimatorStateInfo stateInfo, int layerIndex)
    {
        BasicCharacter.Instance.onAttack -= OnAttack;
        BasicCharacter.Instance.canReceiveInput = false;
    }
}