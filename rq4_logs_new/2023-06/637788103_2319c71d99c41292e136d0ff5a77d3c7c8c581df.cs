using System;
using UnityEngine;
using UnityEngine.InputSystem;

namespace _Scripts.Ragdoll_Movement
{
	[RequireComponent(typeof(Animator))]
	public class ChestRotation : MonoBehaviour
	{
		[SerializeField] private InputActionAsset _keyMap;
		private Animator _animator;
		private InputActionMap _actionMap;
		private InputAction _rotate;
		
		
		private void Awake()
		{
			_animator = GetComponent<Animator>();
			
			if (_keyMap == null) Debug.LogError("Keymap not set");
			
			
			_actionMap = _keyMap.FindActionMap("Player");
			if (_actionMap == null) Debug.LogError("Player action map not found");
			else
			{
				_rotate = _actionMap.FindAction("Rotate");
				if (_rotate == null) Debug.LogError("Rotate action not found");
				else _rotate.Enable();
			}
		}

		private void FixedUpdate()
		{
			float x = _rotate.ReadValue<Vector2>().x;
			
			_animator.SetFloat("Chest", x + 1);
		}
	}
}