using System.Collections;
using System.Collections.Generic;
using UnityEngine;

namespace Andrea
{
    public class CameraFollow : MonoBehaviour
    {
        private const float Y_ANGLE_MIN = 0.0f;
        private const float Y_ANGLE_MAX = 50.0f;
        private const float DISTANCE_MIN = 5.0f;
        private const float DISTANCE_MAX = 10.0f;

        public Transform lookAt;
        public Transform camTransform;

        private Camera camera;

        private float distance = 10.0f;
        private float currentX = 0.0f;
        private float currentY = 20.0f;
        private float sensitivityX = 4.0f;
        private float sensitivityY = 1.0f;

        private void Start()
        {
            camTransform = transform;
            camera = Camera.main;

        }

        private void Update()
        {
            
            
            currentX -= Input.GetAxis("Horizontal") / 2;
            currentY += Input.GetAxis("Vertical") / 2;
            distance -= Input.GetAxis("Mouse ScrollWheel");

            currentY = Mathf.Clamp(currentY, Y_ANGLE_MIN, Y_ANGLE_MAX);
            distance = Mathf.Clamp(distance, DISTANCE_MIN, DISTANCE_MAX);
        }

        private void LateUpdate()
        {
            Vector3 dir = new Vector3(0, 0, -distance);
            Quaternion rotation = Quaternion.Euler(currentY, currentX, 0);
            camTransform.position = lookAt.position + rotation * dir;
            camTransform.LookAt(lookAt.position);
        }

        /*public Transform playerObject;
        public float distanceFromObject = 6f;
        void Update()
        {
            Vector3 lookOnObject = playerObject.position - transform.position;
            lookOnObject = playerObject.position - transform.position;
            transform.forward = lookOnObject.normalized;
            Vector3 playerLastPosition;
            playerLastPosition = playerObject.position - lookOnObject.normalized * distanceFromObject;
            playerLastPosition.y = playerObject.position.y + distanceFromObject / 2;
            transform.position = playerLastPosition;
        }*/
    }
}