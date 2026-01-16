using UnityEngine;
using System.Collections;
using System;

using Plugins.Curves;

namespace Checkers
{
    public class CameraFly : MonoBehaviour
    {
        public Transform lookTarget;
        public Vector3 offset = new Vector3(-10f, 0, 0f);
        public BezierSpline spline;
        public float duration = 1;
        public SplineWalkerMode mode;

        private float progress;

        // Debugging
        private void OnDrawGizmosSelected()
        {
            Gizmos.color = Color.red;
            Gizmos.DrawWireSphere(transform.position, .1f);
        }

        private void Update()
        {
            // Increment the progress via duration
            progress += Time.deltaTime / duration;
            // Is the progress finished?
            if (progress > 1f)
            {
                // Is the mode once only?
                if (mode == SplineWalkerMode.Once)
                {
                    // Cap the progress
                    progress = 1f;
                }
                // else If the mode is in Loop?
                else if (mode == SplineWalkerMode.Loop)
                {
                    // Start back at zero
                    progress -= 1f;
                }
                else
                {
                    // If all else, do ping pong
                    progress = 2f - progress;
                }
            }
            // Get the current spline point based on progress
            transform.localPosition = spline.GetPoint(progress);
        }
        private void LateUpdate()
        {
            // If there is a set look target
            if (lookTarget)
            {
                // Look at that transform (rotate to transform)
                transform.LookAt(lookTarget);
                // Then apply offset (only on y)
                transform.rotation *= Quaternion.AngleAxis(offset.y, Vector3.up);
            }
        }
    }
}