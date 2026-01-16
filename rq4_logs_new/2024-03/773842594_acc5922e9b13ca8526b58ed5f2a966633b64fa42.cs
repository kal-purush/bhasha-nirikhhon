using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class WoodBar : MonoBehaviour
{
    public Transform startPoint; 
    public Transform endPoint;
    public float moveSpeed = 0.1f;

    private bool isMoving = false;

    private void OnEnable()
    {
        SwitchHandle.onIsSwitched += MoveWoodBar;
    }

    

    private void OnDisable()
    {
        SwitchHandle.onIsSwitched -= MoveWoodBar;
    }



    private void MoveWoodBar()
    {
        if (!isMoving)
        {
            StartCoroutine(MoveToTarget());
        }
        
    }

    IEnumerator MoveToTarget()
    {
        isMoving = true;

        while (transform.position != endPoint.position)
        {

            transform.position = Vector3.MoveTowards(startPoint.transform.position, endPoint.transform.position, moveSpeed);
            yield return null; 
        }

        isMoving = false; 
    }
}