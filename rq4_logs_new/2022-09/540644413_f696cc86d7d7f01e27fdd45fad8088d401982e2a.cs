using System.Collections;
using System.Collections.Generic;
using UnityEngine;

namespace net.EthanTFH.BTSGameJam {
    public class CameraControls : MonoBehaviour
    {
        [field: Header("Camera Settings")]
        [field: SerializeField, Tooltip("Maximum Distance The Camera Can Move From The Player"), Range(5, 20)]
        protected float cameraMaxDistance = 10f;
        [field: SerializeField, Tooltip("Minimum Distance The Camera Can Move From The Player"), Range(3, 15)]
        protected float cameraMinDistance = 3f;
        [field: Tooltip("Mouse Sensitivity Float")]
        public float mouseSensitivity = 3.0f;
        [field: Tooltip("Max and Minimum Mouse Position Float"), Range(-100, 100)]
        public float maxX = 40, minX = -40;
        [field: SerializeField, Tooltip("Smoothing Velocity Vector3")]
        private Vector3 smoothVelocity = Vector3.zero;
        [field: SerializeField, Tooltip("Smoothing Time Float")]
        private float smoothTime = 0.2f;
        [field: SerializeField, Tooltip("Will the game capture the users mouse")]
        private bool doCaptureMouse = true;

        [field: Header("Objects")]
        [field: SerializeField, Tooltip("Camera Object")]
        private Camera cam;

        private Transform targetPlayer;
        private float distanceFromTarget = 4.0f;
        private float rotationY, rotationX;
        private Vector3 currentRotation;

        void Start()
        {
            if (doCaptureMouse)
                Cursor.lockState = CursorLockMode.Confined;
            else
                Cursor.lockState = CursorLockMode.None;

            if (cam == null)
                cam = Camera.main;
            if (targetPlayer == null && PlayerController.LocalPlayerInstance != null)
                targetPlayer = PlayerController.LocalPlayerInstance.transform;
        }

        void FixedUpdate()
        {
            if (targetPlayer == null || targetPlayer.transform == null)
            {
                Debug.Log("Attemting to find the target player.");
                if(PlayerController.LocalPlayerInstance)
                    targetPlayer = PlayerController.LocalPlayerInstance.transform;
                return;
            }

            float mouseX = 0f, mouseY = 0f;

            if (Input.GetMouseButton(1))
            {
                // Update Y Rotation around player.
                mouseX = Input.GetAxis("Mouse X") * mouseSensitivity;
                mouseY = Input.GetAxis("Mouse Y") * mouseSensitivity;
            }

            distanceFromTarget -= Input.mouseScrollDelta.y;
            distanceFromTarget = Mathf.Clamp(distanceFromTarget, 3.0f, 10.0f);

            rotationY += mouseX;
            rotationX += mouseY;

            rotationX = Mathf.Clamp(rotationX, minX, maxX);

            Vector3 nextRotation = new Vector3(rotationX, rotationY);
            currentRotation = Vector3.SmoothDamp(currentRotation, nextRotation, ref smoothVelocity, smoothTime);
            transform.localEulerAngles = currentRotation;

            transform.position = targetPlayer.position - transform.forward * distanceFromTarget;
        }
    }
}