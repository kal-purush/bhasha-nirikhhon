using UnityEngine;
using System.Collections;

public class Lerper : MonoBehaviour {

    public bool Lerping = false;

    private Vector3 StartPos;
    private Vector3 TargetPos;
    private float Speed;

    private float Distance;
    private float StartTime;

	public void StartLerp(Vector3 startPos, Vector3 targetPos, float speed)
    {
        StartPos = startPos;
        TargetPos = targetPos;
        Speed = speed;

        Distance = Vector3.Distance(startPos, targetPos);
        StartTime = Time.time;

        Lerping = true;
    }

    public void Update()
    {
        if (Lerping == true)
        {
            float disCovered = (Time.time - StartTime) * Speed;
            float ratio = disCovered / Distance;

            transform.position = Vector3.Lerp(StartPos, TargetPos, ratio);

            if (ratio >= 1)
            {
                Lerping = false;
            }
        }
  
    }
}