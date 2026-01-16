using UnityEngine;

public class FollowCamera_boo : MonoBehaviour
{
    public Transform target;
    public Vector3 offset = new Vector3(0, 3, -7);  // 적당한 거리 유지
    public float smoothSpeed = 0.3f;                // 부드럽게 따라가기

    void LateUpdate()
    {
        Vector3 desiredPosition = target.position + offset;
        Vector3 smoothedPosition = Vector3.Lerp(transform.position, desiredPosition, smoothSpeed);
        transform.position = smoothedPosition;
    }
}