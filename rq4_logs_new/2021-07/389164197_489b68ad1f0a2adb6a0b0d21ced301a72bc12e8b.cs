using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Obstacle1_Behavior : MonoBehaviour
{
    [SerializeField] Transform startPoint;
    [SerializeField] Transform endPoint;
    [SerializeField] private float travelTime;

    private Rigidbody rigid;
    private Vector3 currentPos;

    // Start is called before the first frame update
    void Start()
    {
        rigid = GetComponent<Rigidbody>();
    }

    void FixedUpdate()
    {
        currentPos = Vector3.Lerp(startPoint.position, endPoint.position,
            Mathf.Cos(Time.time / travelTime * Mathf.PI * 2) * -0.5f * 0.5f);
        rigid.MovePosition(currentPos);
    }
}