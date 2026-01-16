#nullable enable
using Unity.VisualScripting.Dependencies.NCalc;
using UnityEngine;

namespace Daipan.Player.Scripts
{
    public class PlayerHolder
    {
        PlayerMono? _player;

        public PlayerMono PlayerMono
        {
            get
            {
                if (_player == null)
                {
                    Debug.LogWarning($"_player is null");
                    return null!;
                }

                return _player;
            }
            set => _player = value;
        }
    }
}