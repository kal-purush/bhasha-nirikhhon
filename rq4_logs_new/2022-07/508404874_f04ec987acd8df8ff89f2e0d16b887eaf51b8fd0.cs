using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using DG.Tweening;

public class DrawOpener : InterractiveItem
{
    private bool isOpen = false;
    private float openingTime = 0.5f;
    private Transform m_Transform;
    private float openingDist;
    private float startingPoint;

    private void Awake()
    {
        m_Transform = transform;
        openingDist = GetComponent<Collider>().bounds.size.z;
        startingPoint = transform.localPosition.z;
    }

    public override void OnIterraction(GameObject sender)
    {
        if (!isOpen)
        {
            m_Transform.DOLocalMoveZ(openingDist,openingTime, false);
        }
        else
        {
            m_Transform.DOLocalMoveZ(startingPoint, openingDist, false);
        }
        isOpen = !isOpen;
    }

}