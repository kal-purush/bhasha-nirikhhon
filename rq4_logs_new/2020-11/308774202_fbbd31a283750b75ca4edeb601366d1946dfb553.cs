using System;
using UnityEngine;

namespace PersonalDevelopment
{
    public enum EnemyState
    {
        Patrol,
        Attack
    }
    
    [RequireComponent(typeof(BoxCollider2D))]
    public abstract class Enemy : MonoBehaviour
    {
        protected EnemyState _state = EnemyState.Patrol;
        protected BoxCollider2D _detectionCollider = null;
        protected bool _playerInRange = false;

        private Vector3 _detectionColliderSize;

        protected virtual void Setup()
        {
            _detectionCollider = GetComponent<BoxCollider2D>();
            _detectionColliderSize = new Vector3(_detectionCollider.size.x, _detectionCollider.size.y, 0);
            _state = EnemyState.Patrol;
        }

        private void OnDrawGizmos()
        {
            Gizmos.color = _playerInRange ? Color.red : Color.blue;
            Gizmos.DrawWireCube(transform.position, _detectionColliderSize);
        }

        private void Awake()
        {
            Setup();
        }
    }
}