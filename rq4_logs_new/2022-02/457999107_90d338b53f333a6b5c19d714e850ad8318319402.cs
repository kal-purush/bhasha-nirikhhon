using Abstractions;
using UnityEngine;

namespace Core
{
    public class MainUnit : MonoBehaviour, ISelectable
    {
        #region Serialize Fields

        [Header("Information")]
        [SerializeField] private string _name;
        [SerializeField] private Sprite _icon;
        
        [Header("Settings")]
        [SerializeField] private float _maxHealth;
        [SerializeField] private float _attackDamage;

        #endregion

        #region Public Properties 

        public float Health => _health;
        public float MaxHealth => _maxHealth;
        public float AttackDamage => _attackDamage;

        public string Name => _name;
        public Sprite Icon => _icon;
        
        public GameObject GameObject => _gameObject;

        #endregion
        
        private float _health;
        private GameObject _gameObject;
        
        public void Start()
        {
            _health = _maxHealth;
            _gameObject = gameObject;
        }
    }
}