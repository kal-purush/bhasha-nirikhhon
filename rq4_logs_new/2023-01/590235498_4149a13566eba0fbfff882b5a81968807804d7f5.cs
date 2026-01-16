using System;
using UnityEngine;

namespace Sandbox.Victor
{
    public class ComponentLook : MonoBehaviour
    {
        [SerializeField, Range(0.0f, 2.0f),]
        private float xSensitivity;
        [SerializeField, Range(0.0f, 2.0f),]
        private float ySensitivity;
        [Header("Camera"), SerializeField,]
        private Transform azimuth;
        [SerializeField]
        private Transform elevation;
        [Header("Clamp Angles"), SerializeField, Range(0, 45),]
        private float maxVerticalAngle;
        [SerializeField, Range(-45, 0),]
        private float minVerticalAngle;

        private ComponentInput _input;

        private Vector2 _turn;

        private void Start()
        {
            if ( !TryGetComponent(out _input) )
                throw new Exception($"Can't find {_input.GetType().Name}");
            _input.InputEventLook += HandleLookInput;

            Cursor.lockState = CursorLockMode.Locked;
        }

        private void OnDestroy()
        {
            _input.InputEventLook -= HandleLookInput;
        }

        private void HandleLookInput( Vector2 lookDirection )
        {
            if(lookDirection.Equals(Vector2.zero)) return;
            _turn.y += lookDirection.y * ySensitivity;
            _turn.x += lookDirection.x * xSensitivity;
            _turn.x = ClampHorizontalRotation(_turn.x);
            _turn.y = ClampVerticalRotation(_turn.y);
            azimuth.localRotation = Quaternion.Euler(0, _turn.x, 0);
            elevation.localRotation = Quaternion.Euler(-_turn.y, 0, 0);
        }

        private float ClampVerticalRotation( float verticalAngle )
        {
            return Mathf.Clamp(verticalAngle, minVerticalAngle, maxVerticalAngle);
        }

        private float ClampHorizontalRotation( float angle )
        {
            return angle switch
            {
                > 360f => angle - 360f,
                < 0f => angle + 360f,
                _ => angle,
            };
        }
    }
}