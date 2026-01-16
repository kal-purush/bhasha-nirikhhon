using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading.Tasks;
using UnityEngine;

public class WoodBar : MonoBehaviour
{
    [SerializeField] Transform startPoint; 
    [SerializeField] Transform endPoint;
    [SerializeField] float moveSpeed = 0.1f;

    [SerializeField] bool isMoving = false;
    [SerializeField] bool isStartPoint = false;

    private void OnEnable()
    {
        SwitchHandle.onIsSwitched += MoveWoodBar;
    }

    

    private void OnDisable()
    {
        SwitchHandle.onIsSwitched -= MoveWoodBar;
    }


    private void Start()
    {
        transform.position = startPoint.position;
    }


    private async void MoveWoodBar()
    {
        if (!isMoving)
        {
            isMoving = true;
            if (isStartPoint)
                await MoveToTarget(endPoint);
            else
                await MoveToTarget(startPoint);
            isStartPoint = !isStartPoint;
            isMoving = false;
        }
    }

    private async Task MoveToTarget(Transform targetPosition)
    {
        var startPos = transform.position;
        var targetPos = targetPosition.position;
        float distance = Vector3.Distance(startPos, targetPos);
        float duration = distance / moveSpeed;
        float startTime = Time.time;

        while (startPos != targetPos)
        {
            float fraction = (Time.time - startTime) / duration;
            transform.position = Vector3.Lerp(startPos, targetPos, fraction);
            await Task.Yield();
            if (this == null) Debug.Log("object deleted"); return;
        }
        transform.position = targetPos;
    }
}