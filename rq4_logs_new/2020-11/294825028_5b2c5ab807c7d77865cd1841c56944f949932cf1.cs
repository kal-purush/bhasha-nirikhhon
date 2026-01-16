using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ArmPivot : MonoBehaviour
{
    // Start is called before the first frame update
    public float maxAngle = 360;
    bool flipRot = false;

    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        Vector2 objectPosition = transform.position;
        Vector2 mousePosition = Camera.main.ScreenToWorldPoint(Input.mousePosition);
        Vector2 direction = mousePosition - objectPosition;

        transform.right = direction;

        if(direction.x < 0)
        {
            flipRot = true;
        }
        else
        {
            flipRot = false;
        }

        Vector3 euler = transform.eulerAngles;

        if(euler.z > 360)
        {
            euler.z -= 360;
        }

        if(!flipRot)
        {
            if (euler.z < 180)
            {
                if (euler.z > maxAngle)
                {
                    euler.z = maxAngle;
                }
            }
            else if (euler.z > 180)
            {
                if (euler.z < 360 - maxAngle)
                {
                    euler.z = 360 - maxAngle;
                }
            }
        }
        else
        {
            if(euler.z < 180)
            {
                if(euler.z < 180 - maxAngle)
                {
                    euler.z = 180 - maxAngle;
                }
            }
            else if(euler.z > 180)
            {        
                if(euler.z > 180 + maxAngle)
                {
                    euler.z = 180 + maxAngle;
                }
            }
            euler.z -= 180;
        }
        
        transform.eulerAngles = euler;

    }
}