using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;


namespace TeamB_TD
{
    namespace Tower
    {
        public class TowerController : MonoBehaviour
        {
            [Header("タワーの体力の最大値")]
            [SerializeField]
            private int _maxLife = 20;

            private int _currentLife = 0;

            public int HP => _currentLife;

            public Action<int> OnLifeChanged; //ライフ変化時に発火するイベント
            public Action OnDead;　//死亡時に発火するイベント

            void Start()
            {
                _currentLife = _maxLife;
            }
            public void Damage()
            {
                _currentLife--;
                OnLifeChanged?.Invoke(_currentLife);
                if (_currentLife == 0)
                    OnDead?.Invoke();
            }
            public void Damage(int x)
            {
                var old = _currentLife;
                _currentLife -= x;
                OnLifeChanged?.Invoke(_currentLife);

                if (old > 0 && _currentLife <= 0)
                    OnDead?.Invoke();
            }
            public void Heal(int x)
            {
                if (_currentLife <= 0) return; // 既にこのタワーが死亡しているときは回復しない。

                _currentLife += x;
                OnLifeChanged?.Invoke(_currentLife);
            }
        }
    }
}