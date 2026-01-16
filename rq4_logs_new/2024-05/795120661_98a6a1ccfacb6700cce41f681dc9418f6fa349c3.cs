#nullable enable
using Daipan.Battle.interfaces;
using Daipan.Enemy.MonoScripts;
using UnityEngine;

namespace Daipan.Enemy.Scripts
{
    public class EnemyHp : IHpSetter
    {
        int _currentHp;
        readonly EnemyMono _enemyMono;

        public EnemyHp(int maxHp, EnemyMono enemyMono)
        {
            CurrentHp = maxHp;
            _enemyMono = enemyMono;
        }

        public int CurrentHp
        {
            get => _currentHp;
            set
            {
                _currentHp = value;
                Debug.Log($"Enemy CurrentHp : {_currentHp}");
                if (_currentHp <= 0) Object.Destroy(_enemyMono.gameObject);
            }
        }
    }
}