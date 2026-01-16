using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CameraController : MonoBehaviour
{
    [SerializeField] Transform target;
    private Vector3 nextPosition;
    [SerializeField] private float smoothness;
    public float maxHeight, minHeight;

    // Updates camera position after player movement
    private void LateUpdate()
    {
        nextPosition =
            new Vector3(transform.position.x, GetNewHeight(), transform.position.z);

        transform.position =
            Vector3.Lerp(transform.position, nextPosition, smoothness * Time.deltaTime);
    }

    // Checks and returns the new target height between the values of maxHeight
    // and minHeight
    private float GetNewHeight()
    {
        if (target.position.y <= maxHeight && target.position.y >= minHeight)
        {
            return target.position.y;
        }

        return transform.position.y;
    }
}