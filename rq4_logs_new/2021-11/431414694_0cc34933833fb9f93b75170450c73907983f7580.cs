using System;
using System.IO;


namespace Exercise10
{
    class Geometry
    {
        public static double CircleArea(double r)
        {
            if (r <=  0)
            {
                throw new InvalidDataException("r - radius can not be negative number");
            }
            return Math.PI * r * 2;
        }
        public static double RectangleArea(double rectangleLength, double rectangleWidth)
        {
            if (rectangleLength <=  0)
            {
                throw new InvalidDataException("l - rectangle length can not be negative number");
            }

            if (rectangleWidth <= 0)
            {
                throw new InvalidDataException("w - rectangle width can not be negative number");
            }

            return rectangleLength * rectangleWidth;
        }
        public static double TriangleArea(double trianleBaseLength, double triangleHeight)
        {
            if (trianleBaseLength <=  0)
            {
                throw new InvalidDataException("Trianle's base length can not be negative number");
            }
            if (triangleHeight <=  0)
            {
                throw new InvalidDataException("Trianle's height can not be negative number");
            }
            return trianleBaseLength * triangleHeight * 0.5;
        }
    }
}