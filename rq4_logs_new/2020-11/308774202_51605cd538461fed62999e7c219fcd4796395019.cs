using Sirenix.OdinInspector;
using UnityEngine;
using UnityEngine.InputSystem;

namespace PersonalDevelopment
{
    public class PlayerInputDebugger : MonoBehaviour
    {
        private const string KeyboardAndMouseWASD = "Keyboard&Mouse_WASD";
        private const string KeyboardAndMouseKeys = "Keyboard&Mouse_Keys";
        private PlayerInput _playerOne = null;
        private PlayerInput _playerTwo = null;
        
        private void Awake()
        {
            StorePlayerInputs();
        }

        private void StorePlayerInputs()
        {
            var allPlayers = FindObjectsOfType<PlayerInput>();
            foreach (var player in allPlayers)
            {
                if (player.playerIndex == 0)
                {
                    _playerOne = player;
                }
                else
                {
                    _playerTwo = player;
                }
            }
        }

        [Button(ButtonSizes.Large), GUIColor(0.4f, 0.8f, 1)]
        [VerticalGroup("Split")]
        [BoxGroup("Split/Player 1")]
        public void PlayerOneKeys()
        {
            _playerOne.SwitchCurrentControlScheme(KeyboardAndMouseKeys, Keyboard.current);
        }
        
        [Button(ButtonSizes.Large), GUIColor(0.4f, 0.8f, 1)]
        [VerticalGroup("Split")]
        [BoxGroup("Split/Player 1")]
        public void PlayerOneWASD()
        {
            _playerOne.SwitchCurrentControlScheme(KeyboardAndMouseWASD, Keyboard.current);
        }
        
        [Button(ButtonSizes.Large), GUIColor(0.4f, 0.8f, 1)]
        [VerticalGroup("Split")]
        [BoxGroup("Split/Player 2")]
        public void PlayerTwoKeys()
        {
            _playerTwo.SwitchCurrentControlScheme(KeyboardAndMouseKeys, Keyboard.current);
        }
        
        [Button(ButtonSizes.Large), GUIColor(0.4f, 0.8f, 1)]
        [VerticalGroup("Split")]
        [BoxGroup("Split/Player 2")]
        public void PlayerTwoWASD()
        {
            _playerTwo.SwitchCurrentControlScheme(KeyboardAndMouseWASD, Keyboard.current);
        }
    }
}