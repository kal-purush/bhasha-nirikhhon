using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public enum HexColor {
    Yellow,
    Blue,
    Purple,
    Black,
    White,
    size
}

public static partial class HexColorExtend {
    public static Color GetColor(this HexColor param){
        switch(param){
            case HexColor.Black:
                return Color.black;
            case HexColor.Blue:
                return Color.blue;
            case HexColor.Purple:
                return Color.red;
            case HexColor.Yellow:
                return Color.yellow;
            case HexColor.White:
                return Color.white;
            default:
                return Color.gray;
        }
    }
}