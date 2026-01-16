using System;
using UnityEngine;

namespace Controller
{
    public class PlayerView : MonoBehaviour
    {
        public event Action moving = () => { };
      
        public Rigidbody Rigidbody;
        
        private PlayerMove _playerMove;
        private void Awake()
        {
            Rigidbody = GetComponent<Rigidbody>();
            _playerMove = new PlayerMove(Rigidbody);
            moving += _playerMove.Move;
            moving += _playerMove.Jump;
        }
        
        private void FixedUpdate()
        {
            moving?.Invoke();
        }
        private void OnCollisionEnter(Collision other)
        {
            Ground(other, true);
        }

        private void OnCollisionExit(Collision other)
        {
            Ground(other, false);
        }

        private void Ground(Collision collision, bool value)
        {
            if (collision.gameObject.tag == ("Ground"))
            {
                _playerMove.inAir = false;
            }
        }
        ~PlayerView()
        {
            moving += _playerMove.Move;
            moving += _playerMove.Jump;
        }
    }
}