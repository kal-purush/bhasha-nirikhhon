using System;
using Microsoft.Xna.Framework;

namespace GigglyLib
{
    public static class VectorExtensions
    {
        public static Vector2 RotateBy(this Vector2 vector, float radians)
        {
            vector = Vector2.Transform(vector, Matrix.CreateRotationZ(radians));
            return vector;
        }

        public static double Range(this Random rand, double min, double max)
        {
            return (rand.NextDouble() * (max - min)) + min;
        }

        public static double Range(this Random rand, double min, double max, double deadzone)
        {
            return rand.Next(0, 2) == 1 ? 
                rand.Range(min, (((max - min) / 2) + min) - deadzone) : 
                rand.Range((((max - min) / 2) + min) + deadzone, max);
        }
    }
}