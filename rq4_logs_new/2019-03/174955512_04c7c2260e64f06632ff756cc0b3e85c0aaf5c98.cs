using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class HexCalculator
{
    float radius;
    Vector3 center;

    public HexCalculator(float _radius, Vector3 _center)
    {
        radius = _radius;
        center = _center;
    }

    public Vector3 PositionFromIndex(int i)
    {
        if (i == 0) return center;
        return Vector3.zero;
    }

    public float GetDegreeFromIndex(int i)
    {
        return 0;
    }

    /// <summary>
    /// a_n = a + nb 型の数列
    /// </summary>
    public int[] CalcProgression(int a, int b, int length)
    {
        var result = new int[length];
        for (int i = 0; i < length; i++)
        {
            result[i] = a + b * i * (i + 1) / 2;
        }
        return result;
    }
}