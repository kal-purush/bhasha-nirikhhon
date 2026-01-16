using UnityEngine;
using System.Collections;

public class AreaController {

    //variable
    Vector2 leftBottom, rightTop;

    public AreaController() {
        leftBottom = Vector2.zero;
        rightTop = Vector2.zero;
    }

    public void SetArea(Vector2 leftBottom, Vector2 rightTop) {
        this.leftBottom = leftBottom;
        this.rightTop = rightTop;
    }

    public void RemoveArea() {
        this.leftBottom = Vector2.zero;
        this.rightTop = Vector2.zero;
    }

    public bool IsInArea(Vector2 origin) {
        if(leftBottom == rightTop) {
            return true;
        }

        return (origin.x >= leftBottom.x && origin.y >= leftBottom.y && origin.x <= rightTop.x && origin.y <= rightTop.y);
    }

    public Vector2 FixPositionInArea(Vector2 origin) {
        if (IsInArea(origin)) {
            return origin;
        }

        Vector2 result = origin;
        if (origin.x < leftBottom.x) {
            result.x = leftBottom.x;
        }
        if (origin.y < leftBottom.y) {
            result.y = leftBottom.y;
        }
        if (origin.x > rightTop.x) {
            result.x = rightTop.x;
        }
        if (origin.y > rightTop.y) {
            result.y = rightTop.y;
        }
        return result;
    }
}