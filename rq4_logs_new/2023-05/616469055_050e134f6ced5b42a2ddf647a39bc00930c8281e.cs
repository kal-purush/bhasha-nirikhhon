using System;
using UnityEngine;

namespace Runtime.AI
{
    [RequireComponent(typeof(AIController))]
    public class AIAnimator : MonoBehaviour
    {
        private AIController _controller;
        private Animator _animator;
        
        private void Awake()
        {
            _controller = GetComponent<AIController>();
            _animator = GetComponentInChildren<Animator>();
        }

        private void Start()
        {
            _controller.onStateChange.AddListener(HandleStateChange);
        }

        private void HandleStateChange(AIState state)
        {
            switch (state)
            {
                case AIState.Roaming:
                    _animator.Play("Enemy Chasing");
                    break;
                case AIState.Searching:
                    _animator.Play("Enemy Searching");
                    break;
                default:
                    _animator.Play("Enemy Roaming");
                    break;
            }
            
        }
    }
}