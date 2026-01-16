using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApplication3
{
    static class Average
    {
        public static double Perimeter(Triangle[] trianglemass)
        {
            int count = 0;
            double perimeter = 0;
            foreach (Triangle triangle in trianglemass)
            {
                if (triangle.Right)
                {
                    count++;
                    perimeter += triangle.GetPerimeter;
                }
            }
            Console.WriteLine("Периметр правильных треугольников: " + perimeter);
            Console.WriteLine("Счетчик " + count);
            return count != 0 ? perimeter / count : 0;
        }

        public static double Area(Triangle[] trianglemass)
        {
            int count = 0;
            double area = 0;
            foreach (Triangle triangle in trianglemass)
            {
                if (triangle.Isosceles)
                {
                    count++;
                    area += triangle.GetArea;
                   
                }
            }
            Console.WriteLine("Площадь равнобедренных треугольников: " + area);
            Console.WriteLine("Счетчик " + count);
       
           return count != 0 ? area / count : 0;
        }
    }
}