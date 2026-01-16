using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CameraFollow : MonoBehaviour
{
    public Transform player;

    public Vector3 tempPosition;

    void Start()
    {
        player = GameObject.FindWithTag("Player").transform;
    }

    // Update is called once per frame
    void LateUpdate()
    {
        if (player == null)
        {
            return;
        }

        tempPosition = transform.position;


        tempPosition.x = player.position.x;
        tempPosition.y = player.position.y;

        transform.position = tempPosition;

    }
}