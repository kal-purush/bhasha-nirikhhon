using System;
using UnityEngine;

namespace PersonalDevelopment
{
    public class BotRangedAttackBehaviour : MonoBehaviour
    {
        public event Action OnPlayerEnteredRange;
        public event Action OnPlayerExitRange;
        
        private BoxCollider _detectionCollider = null;
        private bool _canAttack = false;
        private GameObject _player = null;
        private bool _isGizmoTriggerEntered = false;

        public void Initialize(BoxCollider collider)
        {
            _detectionCollider = collider;
        }
        
        public void Attack()
        {
            //Add Shooting Mechanics here
        }
        
        #region PlayerDetection
        
        private void OnTriggerEnter(Collider other)
        {
            if (other.CompareTag("Player"))
            {
                if (OnPlayerEnteredRange != null)
                {
                    OnPlayerEnteredRange();
                }
                _player = other.gameObject;
                _isGizmoTriggerEntered = true;
            }
        }

        private void OnTriggerExit(Collider other)
        {
            if (other.CompareTag("Player"))
            {
                if (OnPlayerExitRange != null)
                {
                    OnPlayerExitRange();
                }
                _canAttack = false;
                _player = null;
                _isGizmoTriggerEntered = false;
            }
        }

        #endregion
        
#if UNITY_EDITOR
        private void OnDrawGizmos()
        {
            if (_detectionCollider != null)
            {
                Gizmos.color = _isGizmoTriggerEntered ? Color.red : Color.blue;
                Gizmos.DrawWireCube(transform.position, _detectionCollider.size);
            }
            
            // Draw a line to player if enemy can detect them and is not blocked by another collider
            if (_canAttack && _player != null)
            {
                Gizmos.color = Color.green;
                Gizmos.DrawLine(transform.position, _player.transform.position);
            }
        }
#endif
    }
}
