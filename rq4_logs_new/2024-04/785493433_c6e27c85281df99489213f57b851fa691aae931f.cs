using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CameraFollow : MonoBehaviour
{
    public Transform target; // 用来引用人物角色的 Transform 组件
    public float smoothSpeed = 0.125f; // 控制相机跟随的平滑度
    public float edgeBuffer = 1.0f; // 人物到达屏幕边缘多少距离时相机开始跟随
    public Vector3 offset; // 相机和人物之间的偏移量，用于调整相机的位置

    private Camera mainCamera;
    void Start()
    {
        mainCamera = Camera.main;
    }
    void FixedUpdate()
    {
        if (target != null)
        {
            Vector3 targetPosition = target.position + offset;
            targetPosition.y = transform.position.y;

            float screenEdge = mainCamera.ScreenToWorldPoint(new Vector3(edgeBuffer, 0f, mainCamera.nearClipPlane)).x;

            if (target.position.x < transform.position.x - screenEdge || target.position.x > transform.position.x + screenEdge)
            {
                float moveSpeed = Mathf.Abs(target.GetComponent<Rigidbody2D>().velocity.x);
                Vector3 desiredPosition = new Vector3(targetPosition.x, transform.position.y, transform.position.z);
                Vector3 smoothedPosition = Vector3.Lerp(transform.position, desiredPosition, smoothSpeed * moveSpeed * Time.fixedDeltaTime);
                transform.position = smoothedPosition;
            }
        }
    }
}