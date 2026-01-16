using System;
using System.Collections;
using UnityEngine;
using UnityEngine.InputSystem;

namespace Code.Scripts.Minigame
{
    public class DragAndDrop3D : MonoBehaviour
    {
        [SerializeField] private float mouseSpeedPhysics = 10.0f;
        [SerializeField] private float mouseSpeed = 0.1f;
        
        private Camera _mainCamera;
        private WaitForFixedUpdate _waitForFixedUpdate = new WaitForFixedUpdate();

        private MinigameControls _controls;

        private Vector3 _velocity = Vector3.zero;

        private void Awake()
        {
            _controls = new MinigameControls();
            _controls.Player.TouchPress.performed += ControlPressed;
        }

        private void Start()
        {
            _mainCamera = Camera.main;
        }

        private void OnEnable()
        {
            _controls.Enable();
        }

        private void OnDisable()
        {
            _controls.Disable();
        }

        private void ControlPressed(InputAction.CallbackContext context)
        {
            var ray = _mainCamera.ScreenPointToRay(_controls.Player.TouchPoint.ReadValue<Vector2>());
            if (!Physics.Raycast(ray, out var hit))
            {
                return;
            }

            Debug.Log(hit.collider.name);

            if (hit.collider != null && hit.collider.gameObject.CompareTag("Draggable"))
            {
                StartCoroutine(DragUpdateTouch(hit.collider.gameObject));
            }
        }

        public virtual IEnumerator DragUpdateTouch(GameObject draggedObject)
        {
            float initialDistance = Vector3.Distance(draggedObject.transform.position, _mainCamera.transform.position);
            draggedObject.TryGetComponent<Rigidbody>(out var rb);
            draggedObject.TryGetComponent<IDrag>(out var iDrag);
            iDrag?.OnDragStart();
            
            while (_controls.Player.TouchPress.ReadValue<float>() != 0)
            {
                var ray = _mainCamera.ScreenPointToRay(_controls.Player.TouchPoint.ReadValue<Vector2>());
                iDrag?.OnDrag();
                if (rb != null)
                {
                    var direction = ray.GetPoint(initialDistance) - draggedObject.transform.position;
                    rb.velocity = direction * mouseSpeedPhysics;
                    yield return _waitForFixedUpdate;
                }
                else
                {
                    draggedObject.transform.position = Vector3.SmoothDamp(draggedObject.transform.position,
                        ray.GetPoint(initialDistance), ref _velocity, mouseSpeed);
                    yield return null;
                }
            }
            iDrag?.OnDragEnd();
        }
    }
}