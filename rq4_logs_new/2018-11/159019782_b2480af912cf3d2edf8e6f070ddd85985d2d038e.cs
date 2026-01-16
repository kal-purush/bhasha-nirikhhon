using System.Collections;
using System.Collections.Generic;
using UnityEngine;

namespace FPS
{
    public class Character : BaseSceneObject, IDamageable
    {
        [SerializeField]
        private float _maxHealth;
        public float MaxHealth => _maxHealth;

        [SerializeField]
        private float _currentHealth;
        public float CurrentHealth => _currentHealth;

        public void ApplyDamage(float damage)
        {
            if (CurrentHealth <= 0) return;
            _currentHealth -= damage;
            //Color = Random.ColorHSV();

            if (CurrentHealth <= 0) Die();
        }

        public void Die()
        {
            //Color = Color.red;
            Collider.enabled = false;
            Rigidbody.useGravity = true;
            Destroy(gameObject, 5f);
        }
    }
}