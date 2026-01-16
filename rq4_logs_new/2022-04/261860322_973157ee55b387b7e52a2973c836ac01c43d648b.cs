using System;
using System.Numerics;

namespace SpringChallenge2022.Models
{
    public static class Vector2Extensions
    {
        public static double GetAngle(this Vector2 direction)
        {
            var radians = Math.Atan2(direction.Y, direction.X);
            var degrees = 180 * radians / Math.PI;
            return (360 + Math.Round(degrees)) % 360;
        }

        public static Vector2 GetDirection(float degree)
        {
            var radians = degree * Math.PI / 180;
            return new Vector2((float)Math.Cos(radians), (float)Math.Sin(radians));
        }
    }
}