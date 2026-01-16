using System;
using Unity.Netcode;
using UnityEngine;

namespace Runtime.Player
{
    public class PatientAnimationController : NetworkBehaviour
    {
        [SerializeField] private Animator animator;
        private static readonly int Horizontal = Animator.StringToHash("Horizontal");
        private static readonly int Vertical = Animator.StringToHash("Vertical");

        private readonly NetworkVariable<float> _horizontalInput = new();
        private readonly NetworkVariable<float> _verticalInput = new();
        

        private void Update()
        {
            if (!IsOwner) return;

            _horizontalInput.Value = Input.GetAxis("Horizontal");
            _verticalInput.Value = Input.GetAxis("Vertical");
            
            animator.SetFloat(Horizontal, Input.GetAxis("Horizontal"));
            animator.SetFloat(Vertical, Input.GetAxis("Vertical"));
        }


        public override void OnNetworkSpawn()
        {
            _horizontalInput.OnValueChanged += (oldValue, newValue) =>
            {
                animator.SetFloat(Horizontal, newValue);
            };

            _verticalInput.OnValueChanged += (oldValue, newValue) =>
            {
                animator.SetFloat(Vertical, newValue);
            };
            base.OnNetworkSpawn();
        }
    }
}