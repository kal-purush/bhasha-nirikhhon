using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Numerics;
using UnityEditor;
using UnityEngine;

public class RotateMill : MonoBehaviour
{
    // Update is called once per frame
    void Update()
    {
        transform.Rotate(0,0, 10f * Time.deltaTime);

    }
}