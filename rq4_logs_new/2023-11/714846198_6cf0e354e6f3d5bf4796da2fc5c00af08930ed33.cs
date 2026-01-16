using System;
using UnityEngine;

namespace TeamB_TD
{
    namespace Battle
    {
        [DefaultExecutionOrder(-90)]
        public class BattleManager : MonoBehaviour
        {
            private static BattleManager _current;
            public static BattleManager Current => _current;

            private BattleStatus _status = BattleStatus.Ready;

            public event Action<int> OnCompletedEnemyCountChanged;

            public BattleStatus Status
            {
                get => _status;
                private set
                {
                    var old = _status;
                    _status = value;
                    if (old != _status) { OnStatusChanged?.Invoke(_status); }
                }
            }

            public event Action<BattleStatus> OnStatusChanged;

            private void Awake()
            {
                _current = this;
            }

            private void GameClear()
            {
                Status = BattleStatus.GameClear;
            }

            private void GameOver()
            {
                Status = BattleStatus.GameOver;
            }

            public enum BattleStatus
            {
                Ready,
                Playing,
                GameOver,
                GameClear,
            }
        }
    }
}