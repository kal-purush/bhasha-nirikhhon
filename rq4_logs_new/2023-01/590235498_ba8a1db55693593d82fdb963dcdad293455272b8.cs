using System;
using System.Collections;
using UnityEngine;

namespace Sandbox.Victor
{
    public class Bullet : Projectile
    {
        private Rigidbody _rigidbody;
        private TrailRenderer _trailRenderer;
        
        [SerializeField]
        private float velocity = 1;

        private float _currentLife = 0.0f;
        [SerializeField]
        public float maxLife = 2.0f;

        private void Awake()
        {
            if ( !TryGetComponent(out _rigidbody) )
                throw new Exception("Can't find rigidbody");
            if(!TryGetComponent(out _trailRenderer))
                throw new Exception("Can't find trailRenderer");
        }

        private void Update()
        {
            _currentLife += Time.deltaTime;
            if ( _currentLife > maxLife )
            {
                Reset();
            }
        }

        private void OnTriggerEnter( Collider other )
        {
            Debug.Log(other.name);
            Reset();
        }

        public override void Fire()
        {
            _trailRenderer.Clear();
            _trailRenderer.emitting = true;
            _trailRenderer.enabled = true;
            _rigidbody.velocity = transform.forward * velocity;
        }

        public override void OnImpact()
        {
        }
        
        public override void Reset()
        {
            _trailRenderer.Clear();
            _trailRenderer.emitting = false;
            _trailRenderer.enabled = false;
            _currentLife = 0.0f;
            _rigidbody.velocity = Vector3.zero;
            ResetEvent?.Invoke();
        }
    }
}