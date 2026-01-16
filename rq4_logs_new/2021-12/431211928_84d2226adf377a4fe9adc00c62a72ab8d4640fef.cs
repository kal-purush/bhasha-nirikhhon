using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class WaterDisappear : MonoBehaviour
{
    bool isLerping = false;
    float journeyLength = 3f; //lerp time
    float timeSinceStartLerping = 0f;
    float yStart = 1f;
    float yTarget = 0f;

    Vector3 startPosition;
    Vector3 targetPosition;



    private void Update()
    {
        timeSinceStartLerping += Time.deltaTime;
        //TestLerp();
        LerpPositions();
    }

    private void TestLerp()
    {
        if (Input.GetKeyDown(KeyCode.L))
        {
            ChangeStatus();
        }
    }

    public void ChangeStatus()
    {


        isLerping = true;
        timeSinceStartLerping = 0f;

        startPosition = new Vector3(transform.position.x, yStart, transform.position.z);
        targetPosition = new Vector3(transform.position.x, yTarget, transform.position.z);

    }



    private void LerpPositions()
    {
        if (!isLerping)
        {
            return;
        }
        if (timeSinceStartLerping > journeyLength)
        {
            isLerping = false;
            return;
        }
        //Debug.Log("Lerping");
        float fractionOfJourney = timeSinceStartLerping / journeyLength;
        //we lerping
        transform.position = Vector3.Lerp(startPosition, targetPosition, fractionOfJourney);
    }
}