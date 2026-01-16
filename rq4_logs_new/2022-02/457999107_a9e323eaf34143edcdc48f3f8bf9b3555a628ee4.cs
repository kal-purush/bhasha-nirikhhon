using Abstractions;
using UnityEngine;

namespace Core
{
    public sealed class MainBuilding : MonoBehaviour, IUnitProducer, ISelectable
    {
        #region Serialize Fields

        [Header("Information")]
        [SerializeField] private string _name;
        [SerializeField] private Sprite _icon;
        
        [Header("Settings")]
        [SerializeField] private float _maxHealth;
        
        [Header("Spawning")]
        [SerializeField] private GameObject _unitPrefab;
        [SerializeField] private Transform _unitsParent;

        #endregion

        #region Public Properties 

        public float Health => _health;
        public float MaxHealth => _maxHealth;
        public Sprite Icon => _icon;
        
        public string Name => _name;
        public GameObject GameObject => _gameObject;

        #endregion
        
        private float _health;
        private GameObject _gameObject;

        public void Start()
        {
            _health = _maxHealth;
            _gameObject = gameObject;
        }
        
        public void ProduceUnit()
        {
            Instantiate(
                _unitPrefab, 
                new Vector3(Random.Range(-10, 10), 0, Random.Range(-10, 10)), 
                Quaternion.identity, _unitsParent);
        }
    }
}