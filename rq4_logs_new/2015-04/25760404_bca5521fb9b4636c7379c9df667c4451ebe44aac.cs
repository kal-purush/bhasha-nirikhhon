using System;
using Microsoft.Xna.Framework;

namespace XnaAdapter
{
    public class PolarCoordinateHelper
    {
        public static Vector2 AngleToVector2(double angle)
        {
            return new Vector2((float)Math.Sin(angle), (float)Math.Cos(angle));
        }
    }
}