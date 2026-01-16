using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.InputSystem;
using UnityEngine.InputSystem.Utilities;
using UnityEngine.UI;

namespace _Scripts
{
    public class FaceOverlayTutorial : MonoBehaviour
    {
        private PlayerInput _input;
        private InputActionMap _map;
        private InputAction _interact;
        private int _prefab, _characterIndex;
        [SerializeField] private Sprite _blorbo, _beepo, _dingo, _glonk, _spaghetto;
        [SerializeField] private List<Color> _colors;
        private Sprite _chosen;
        private Image _portrait, _circle;

        private void OnEnable()
        {
            _circle = transform.GetChild(0).GetComponent<Image>();
            _portrait = transform.GetChild(1).GetComponent<Image>();
        }

        public void Initialize(int prefab)
        {
            gameObject.SetActive(true);
            _prefab = prefab;
            _circle.color = _colors[_characterIndex];
            switch (_prefab)
            {
                case 0:
                    _chosen = _blorbo;
                    break;
                case 1:
                    _chosen = _beepo;
                    break;
                case 2:
                    _chosen = _dingo;
                    break;
                case 3:
                    _chosen = _glonk;
                    break;
                case 4:
                    _chosen = _spaghetto;
                    break;
            }
            _portrait.sprite = _chosen;
            Color color = _portrait.color;
            color.a = 0.5f;
            _portrait.color = color;
            _map = _input.actions.FindActionMap("Player");
            _interact = _map.FindAction("interaction");
            StartCoroutine(ReadyUpInteraction());
        }

        public IEnumerator ReadyUpInteraction()
        {
            yield return null;
            _interact.performed += ReadyUp;
        }

        private void OnDisable()
        {
            _interact.performed -= ReadyUp;
        }

        public void ReadyUp(InputAction.CallbackContext context)
        {
            Color color = _portrait.color;
            color.a = 0.5f;
            _portrait.color = color;
            Debug.Log("Readied up");
            EventBus<PlayerReadyUpTutorialEvent>.Publish(new PlayerReadyUpTutorialEvent());
        }

        public void SetPlayerInput(PlayerInput playerInput)
        {
            _input = playerInput;
        }
    }
}