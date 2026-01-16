using System;
using UnityEngine;
using UnityEngine.InputSystem;
using UnityEngine.UI;

namespace _Scripts
{
	[RequireComponent(typeof(Image))]
	[RequireComponent(typeof(Button))]
	[RequireComponent(typeof(PlayerInput))]
	public class PlayAgainButton : MonoBehaviour
	{
		private Button _button;
		private PlayerInput _playerInput;
		private InputActionMap _map;
		private InputAction _interact;
		private Image _buttonImage;

		private void Awake()
		{
			_button = GetComponent<Button>();
			_buttonImage = GetComponent<Image>();
			_playerInput = GetComponent<PlayerInput>();
		}

		private void Start()
		{
			_map = _playerInput.actions.FindActionMap("Player");
			_interact = _map.FindAction("interaction");
			_interact.started += SwitchButtonPressedActive;
			_interact.canceled += SwitchButtonPressedUnActive;
		}

		private void OnDisable()
		{
			_interact.started -= SwitchButtonPressedActive;
			_interact.canceled -= SwitchButtonPressedUnActive;
		}

		private void SwitchButtonPressedActive(InputAction.CallbackContext context)
		{
			_buttonImage.sprite = _button.spriteState.pressedSprite;
		}
        
		private void SwitchButtonPressedUnActive(InputAction.CallbackContext context)
		{
			_buttonImage.sprite = _button.spriteState.highlightedSprite;
			_button.onClick.Invoke();
		}
	}
}