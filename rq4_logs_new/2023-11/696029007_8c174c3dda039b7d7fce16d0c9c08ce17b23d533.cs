using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ResetPositionIfOOB : MonoBehaviour
{
    // Start is called before the first frame update
    Vector3 originalPosition;
    public Bounds bounds;

    void Start()
    {
        originalPosition = transform.position;    
    }

    // Update is called once per frame
    void Update()
    {
        if (!bounds.Contains(transform.position))
        {
            transform.position = originalPosition;
            Debug.Log("moving");
        }
        else
        {
            Debug.Log("not moving");
        }
    }
}

// Bounds(-14, 21, 4.69999981, 1, 10, 1.39999998)

//UnityEditor.TransformWorldPlacementJSON:{"position":{"x":6.300000190734863,"y":3.2100000381469728,"z":4.699999809265137},"rotation":{"x":0.0,"y":0.0,"z":0.0,"w":1.0},"scale":{"x":27.729999542236329,"y":10.0,"z":29.889999389648439}}