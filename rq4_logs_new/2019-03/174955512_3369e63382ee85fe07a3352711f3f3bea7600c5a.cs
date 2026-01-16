using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public struct RPoint
{
    public int x, y;
    public RPoint(int _x, int _y)
    {
        x = _x;
        y = _y;
    }

    public static RPoint OneX = new RPoint(1, 0);
    public static RPoint OneY = new RPoint(0, 1);
    public static RPoint Zero = new RPoint(0, 0);

    public static RPoint operator +(RPoint a, RPoint b)
    {
        return new RPoint(a.x + b.x, a.y + b.y);
    }
    public static RPoint operator -(RPoint a, RPoint b)
    {
        return new RPoint(a.x - b.x, a.y - b.y);
    }
    public static RPoint operator -(RPoint a)
    {
        return new RPoint(-a.x, -a.y);
    }
    public static bool operator ==(RPoint a, RPoint b)
    {
        if (a == null || b == null) return false;
        if (a.x == b.x && a.y == b.y) return true;
        return false;
    }
    public static bool operator !=(RPoint a, RPoint b) => !(a == b);
}