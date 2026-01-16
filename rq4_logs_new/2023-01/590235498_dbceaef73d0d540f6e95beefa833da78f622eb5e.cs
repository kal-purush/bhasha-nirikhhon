using System;
using UnityEngine;
using UnityEngine.Animations.Rigging;

namespace Player
{
    public class ComponentTarget : MonoBehaviour
    {
        private Camera _mainCamera;
        private Transform _cameraTransform;
        [SerializeField]
        private Transform targetSphere;

        public Vector3 CurrentTarget { get; private set; }

        private void Start()
        {
            _mainCamera = Camera.main;
            if ( _mainCamera == null )
                throw new Exception("Can't find main camera...");

            _cameraTransform = _mainCamera.transform;
        }

        private void Update()
        {
            if ( !Physics.Raycast(_cameraTransform.position, _cameraTransform.forward, out RaycastHit hit,
                    Mathf.Infinity) )
            {
                CurrentTarget = _cameraTransform.forward * 100.0f;
            }
            else
            {
                CurrentTarget = hit.point;
            }

            targetSphere.position = CurrentTarget;
        }
    }
}