using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using UnityEngine;

class ViewportUtility
{
    public static bool IsInView(Vector3 point)
    {
        var topLeft = GetViewportTopLeft();
        var bottomRight = GetViewportBottomRight();

        return point.x > topLeft.x && point.x < bottomRight.x &&
            point.y > bottomRight.y && point.y < topLeft.y;
    }

    static public Vector2 GetViewportTopLeft()
    {
        Vector3 worldPoint = Camera.main.ViewportToWorldPoint(new Vector3(0, 1, 0));

        return new Vector2(worldPoint.x, worldPoint.y);
    }

    static public Vector2 GetViewportBottomRight()
    {
        Vector3 worldPoint = Camera.main.ViewportToWorldPoint(new Vector3(1, 0, 0));

        return new Vector2(worldPoint.x, worldPoint.y);
    }
}