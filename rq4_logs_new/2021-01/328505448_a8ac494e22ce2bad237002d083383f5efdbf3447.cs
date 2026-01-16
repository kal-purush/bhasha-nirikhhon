using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class BigStick : MonoBehaviour
{
    private bool isPressed = false;

    private Vector2 startPos;
    private Vector2 endPos;
    public float step;
    private float progress;

    void Start()
    {
        startPos = transform.position;
        endPos = new Vector2(0.0f, 1.0f);
    }

    public void OnMouseDown()
    {
        isPressed = true;
    }

    public void OnMouseUp()
    {
        isPressed = false;
    }

    // Update is called once per frame
    void FixedUpdate()
    {
       if(isPressed)
        {
            transform.position = Vector2.Lerp(transform.position, startPos + endPos, progress);
            progress += step;
        }
        else
        {
            transform.position = Vector2.Lerp(transform.position, startPos, progress);
            progress += step;
        }
    }
}