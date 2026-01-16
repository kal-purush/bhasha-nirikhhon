using UnityEngine;
using UnityEngine.InputSystem;

namespace PersonalDevelopment
{
    public class PlayerSelectionBehaviour : MonoBehaviour
    {
        private PlayerInput _input = null;
        [SerializeField] private GameObject _playerSelectionGameObject = null;
        [SerializeField] private GameObject _playerJoinText = null;
        [SerializeField] private Vector3 _playerPlacement = Vector3.zero;
        public void Initialize(PlayerInput input)
        {
            _input = input;
            _input.GetComponent<PlayerBehaviour>().SetPlayerModel();
            _playerSelectionGameObject.SetActive(true);
            _playerJoinText.SetActive(false);
            SetPlayerModelPosition();
        }

        private void SetPlayerModelPosition()
        {
            _input.transform.position = _playerPlacement;
        }
        
        #region mono

        private void OnEnable()
        {
            _playerSelectionGameObject.SetActive(false);
            _playerJoinText.SetActive(true);
        }

        #endregion
    }
}