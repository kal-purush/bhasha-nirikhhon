using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class HexCoordinate
{
    readonly RPoint[] TargetPoint =
    {
        new RPoint(0, 2),
        new RPoint(1, 1),
        new RPoint(1, -1),
        new RPoint(0, -2),
        new RPoint(-1, -1),
        new RPoint(-1, 1)
    };

    Hex[,] hices;
    int width, height;

    public HexCoordinate(int _width, int _height)
    {
        width = _width;
        height = _height;
        hices = new Hex[width * 2 + 1, height * 2 + 1];
    }

    public Hex[] GetContactedHex(RPoint point)
    {
        var contactedArray = new Hex[6];
        for (int i = 0; i < 6; i++)
        {
            contactedArray[i] = TryGetHex(point + TargetPoint[i]);
        }
        return contactedArray;
    }

    public void SetHex(Hex hex)
    {
        if (GetHex(hex.Point) != null) throw new System.Exception("Tried to set Hex where Hex already set.");
        var index = ConvertRPointToIndex(hex.Point);
        hices[index.x, index.y] = hex;
    }

    public Hex GetHex(RPoint point)
    {
        var index = ConvertRPointToIndex(point);
        return hices[index.x, index.y];
    }

    public Hex TryGetHex(RPoint point)
    {
        if (Mathf.Abs(point.x) > width) return null;
        if (Mathf.Abs(point.y) > height) return null;
        return GetHex(point);
    }

    private RPoint ConvertRPointToIndex(RPoint point) => new RPoint(point.x + width, point.y + height);
}