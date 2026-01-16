using UnityEngine;

namespace Shaders
{
    public class GhostWallSync : MonoBehaviour
    {
        public static int PosID = Shader.PropertyToID("_Position");
        public static int SizeID = Shader.PropertyToID("_Size");

        public Material wallMat;
        public Camera cam;
        public LayerMask mask;

        private float _desiredSize = 0f;
        private float _currentSize = 0f;
        private float _animationSpeed = 5f;

        private void Update()
        {
            var lookUpPos = (transform.position - cam.transform.position) * 1.0025f;
            var dir = cam.transform.position - lookUpPos;
            var ray = new Ray(lookUpPos, dir.normalized);

            if (Physics.Raycast(ray, Mathf.Infinity, mask))
            {
                _desiredSize = 1f;
                _animationSpeed = 5f;
            }
            else
            {
                _desiredSize = 0f;
                _animationSpeed = 2f;
            }

            _currentSize = Mathf.Lerp(_currentSize, _desiredSize, Time.deltaTime * _animationSpeed);
            wallMat.SetFloat(SizeID, _currentSize);

            var view = cam.WorldToViewportPoint(transform.position);
            wallMat.SetVector(PosID, view);
        }
    }
}