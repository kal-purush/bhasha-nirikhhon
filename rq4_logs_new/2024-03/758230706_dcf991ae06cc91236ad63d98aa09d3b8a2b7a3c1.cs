using UnityEngine;

public class CameraFollow : MonoBehaviour
{
    public GameObject target; // Reference to the character's GameObject
    public float smoothSpeed = 0.125f; // Speed at which the camera follows the character
    public Vector3 offset; // Offset of the camera from the character

    void FixedUpdate()
    {
        if (target != null)
        {
            Vector3 desiredPosition = target.transform.position + offset;
            Vector3 smoothedPosition = Vector3.Lerp(transform.position, desiredPosition, smoothSpeed);
            transform.position = smoothedPosition;
        }
    }
}