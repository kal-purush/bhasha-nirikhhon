using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CameraMove : MonoBehaviour
{
    Transform playerTansform;
    Vector3 offset;

    void Awake()
    {
        playerTansform = GameObject.FindGameObjectWithTag("Player").transform;
        offset = transform.position - playerTansform.position;
    }

    void LateUpdate()
    {
        transform.position = playerTansform.position + offset;
    }
}