using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class BladeSpin : MonoBehaviour {

    public float speed;

    private void Start()
    {
        speed = 300f;
    }

    void Update()
    {
        transform.Rotate(Vector3.forward, speed * Time.deltaTime);
    }
}