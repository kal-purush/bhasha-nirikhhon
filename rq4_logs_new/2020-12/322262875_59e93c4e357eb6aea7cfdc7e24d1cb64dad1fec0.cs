using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class StopJumpTransition : Transition
{
    [SerializeField] private Animator _animator;

    //private float _runTime;
    //private float _animationLength;

    //private void Start()
    //{
    //    _runTime = 0f;
    //}

    private void Update()
    {
        if (/*_animator.GetCurrentAnimatorStateInfo(0).IsName("jump") && */_animator.GetCurrentAnimatorStateInfo(0).normalizedTime >= 1.0f)
        {
            NeedTransit = true;
        }
    }
}