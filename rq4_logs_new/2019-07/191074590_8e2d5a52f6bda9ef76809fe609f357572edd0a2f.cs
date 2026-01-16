using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CameraMove : MonoBehaviour
{
    [SerializeField, Header("追従ターゲット")]
    Transform target;
    [SerializeField, Header("追従時間")]
    float smoothTime = 1.0f;

    Vector3 camPos;
    Vector3 velocity;

    // Start is called before the first frame update
    void Start()
    {
        camPos = transform.position;
    }

    // Update is called once per frame
    void FixedUpdate()
    {
        Vector3 vec = target.position + camPos;
        vec.y = camPos.y;
        transform.position = Vector3.SmoothDamp(transform.position, vec, ref velocity, smoothTime);
    }
}