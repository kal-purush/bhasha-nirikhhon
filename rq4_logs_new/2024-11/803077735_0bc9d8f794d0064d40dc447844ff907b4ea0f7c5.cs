using System;
using System.Collections;
using System.Collections.Generic;
using Unity.Collections;
using UnityEngine;
using UnityEngine.UIElements;

public class FishHealthBarPosition : MonoBehaviour
{
    private GameObject ParentFish;
    private Vector3 HealthBarPosition;
    private Vector3 FishPosition;
    private float FishPositionX;
    private float FishPositionY;

    void Start()
    {
        ParentFish = transform.root.gameObject;
    }

    void Update()
    {
        FishPosition = ParentFish.transform.position;
        FishPositionX = FishPosition.x;
        FishPositionY = FishPosition.y;

        transform.position = new Vector3(FishPositionX, FishPositionY + 0.75f, 2);
        transform.rotation = Quaternion.identity;
    }
}