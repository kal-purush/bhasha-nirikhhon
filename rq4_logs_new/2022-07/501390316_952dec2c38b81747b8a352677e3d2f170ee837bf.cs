using UnityEngine;

namespace GB_Platformer
{
    [System.Serializable]
    internal sealed class Health
    {
        [SerializeField] private float _maxHealth = 100f;

        private float _currentHealth;

        public float MaxHealth { get => _maxHealth; set => _maxHealth = value; }
        public float CurrentHealth
        {
            get => _currentHealth;
            set => _currentHealth = Mathf.Clamp(value, 0, _maxHealth);
        }

        public Health(float heath)
        {
            _currentHealth = heath;
        }
    }
}