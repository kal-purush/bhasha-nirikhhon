using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ApproachingDanger : MonoBehaviour
{
    [SerializeField] float speed;
    [SerializeField] float acceleration;
    Transform candle;

    public Transform Candle { get => candle; set => candle = value; }

    // Update is called once per frame
    void Update()
    {
        speed += acceleration * Time.deltaTime;
        transform.position = Vector3.MoveTowards(transform.position, candle.position, speed * Time.deltaTime);
    }
}