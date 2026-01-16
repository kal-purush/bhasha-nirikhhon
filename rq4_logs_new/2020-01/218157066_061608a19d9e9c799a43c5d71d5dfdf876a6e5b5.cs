using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Cloud_Move : MonoBehaviour
{
    private Vector3 pos1;
    private Vector3 pos2;
    public Vector3 posDiff= new Vector3 (0f, 0f, 0f);
    public float speed = 0.5f;
 
    void Start() {
        pos1 = transform.position;
        pos2 = transform.position + posDiff;
    }
 
    void Update () {
        transform.position = Vector3.Lerp(pos1, pos2, Mathf.PingPong(Time.time*speed, 1.0f));
    }
 
} 
