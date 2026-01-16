using UnityEngine;

namespace PersonalDevelopment
{
    [RequireComponent(typeof(Rigidbody))]
    public class PlayerBehaviour : MonoBehaviour
    {
        private enum PlayerState
        {
            Menu,
            Game
        }
        
        private Rigidbody _rigidbody = null;
        private PlayerState _state = PlayerState.Menu;
        
        private PlayerState State
        {
            get => _state;
            set
            {
                _state = value;
                OnStateSet();
            }
        }


        private void Awake()
        {
            _rigidbody = GetComponent<Rigidbody>();
            State = PlayerState.Menu;
        }

        private void OnStateSet()
        {
            switch (State)
            {
                case PlayerState.Menu:
                    OnMenuStateSet();
                    break;
                case PlayerState.Game:
                    OnGameStateSet();
                    break;
            }
        }

        private void OnMenuStateSet()
        {
            _rigidbody.isKinematic = true;
        }

        private void OnGameStateSet()
        {
            _rigidbody.isKinematic = false;
        }

    }
}