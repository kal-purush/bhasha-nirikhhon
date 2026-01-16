using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class FireballMotion1 : MonoBehaviour
{

    public float moveTime;
    public AnimationCurve movementCurve;

    public Transform destTransform;
    private Vector3 destination;
    private Vector3 startPosition;


    private bool forward = true;
    private bool moving;
    
    // Start is called before the first frame update
    void Start()
    {
        startPosition = transform.localPosition;
        destination = destTransform.localPosition;
    }

    // Update is called once per frame
    void Update()
    {
        if (!moving)
        {
            if (forward)
            {
                StartCoroutine(MoveAbout(startPosition, destination));
            }
            else
            {
                StartCoroutine(MoveAbout(destination, startPosition));
            }
            moving = true;
        }
    }

    IEnumerator MoveAbout(Vector3 start, Vector3 end)
    {
        float t = 0;

        while (t < 1f)
        {
            t += Time.deltaTime / moveTime;

            transform.localPosition = Vector3.Lerp(start, end, movementCurve.Evaluate(t));
            yield return null;
        }


        forward = !forward;
        moving = false;
    }
}