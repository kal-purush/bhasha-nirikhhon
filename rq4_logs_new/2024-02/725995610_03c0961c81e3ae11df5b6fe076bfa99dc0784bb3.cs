#nullable enable

using DataskopAR.Utils;
using UnityEngine;

namespace DataskopAR.Entities {

    public class FaceCamera : MonoBehaviour {

        [SerializeField] [Tooltip("How close the camera has to be to the object for it to face it.")]
        private float faceThreshold;

        [SerializeField] private Camera? targetCamera;


        // ReSharper disable once Unity.NoNullPropagation
        private Transform? TargetTransform => targetCamera?.transform;

        private void AlignWith(Vector3 target) {

            var diff = (target - transform.position).WithY(0);
            float distance = diff.magnitude;
            bool shouldFace = distance <= faceThreshold;

            if (!shouldFace) return;

            transform.forward = diff.normalized;
        }

        private void Update() {
            var targetTransform = TargetTransform;
            // ReSharper disable once Unity.PerformanceCriticalCodeNullComparison
            if (targetTransform == null) return;

            AlignWith(targetTransform.position);
        }

        private void Awake() {
            if (targetCamera == null)
                targetCamera = Camera.main;
        }
    }
}