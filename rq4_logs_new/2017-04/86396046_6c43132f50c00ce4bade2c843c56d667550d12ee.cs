using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class SpriteSize : MonoBehaviour
{

    public float xPercentage;
    public float yPercentage;

    float x;
    float y;
    float originalScaleX;
    float originalScaleY;
    bool Updated = false;

    void Awake()
    {
       // xPercentage /= 100.0f;
        //yPercentage /= 100.0f;

        originalScaleX = transform.localScale.x;
        originalScaleY = transform.localScale.y;
        Resize();
        Debug.Log("Original SCALE (AWAKE)" + originalScaleX + " " + originalScaleY);
    }

    void Resize()
    {
        if (!Updated)
        {
            SpriteRenderer sr = GetComponent<SpriteRenderer>();
            x = sr.sprite.bounds.size.x;
            y = sr.sprite.bounds.size.y;
            Debug.Log("Screen " + Screen.width);
            Debug.Log("True size of sprite " + x);
            Debug.Log("Original scale * percentage " + originalScaleX * xPercentage);
            Debug.Log("Percentage " + (100.0f * x / Screen.width));
            float scaleX = originalScaleX * xPercentage / (100.0f * x / Screen.width);
            float scaleY = originalScaleY * yPercentage / (100.0f * y / Screen.height);

            if (scaleX < scaleY)
                scaleY = scaleX;
            else if (scaleY < scaleX)
                scaleX = scaleY;
            Vector3 wantedScale = new Vector3(scaleX, scaleY);
            Debug.Log("WANTED SCALE" + wantedScale);
            transform.localScale = wantedScale;
        }
    }

    void Update()
    {
        if (!Updated)
        {
            Resize();
            Updated = true;
            Debug.Log("(UPDATE)Scale in button script directly" + transform.localScale.x);
        }
    }
}