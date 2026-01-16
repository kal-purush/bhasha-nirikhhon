using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Homework2_4
{
    /// <summary>
    /// класс фигура
    /// </summary>
    public class Figure
    {
        /// <summary>
        /// точки из которых состоит фигура
        /// </summary>
        public Point[] Points { get; private set; }

        /// <summary>
        /// Имя фигуры
        /// </summary>
        public string Name { get; private set; }

        /// <summary>
        /// констрктор с 3 параметрами - точками
        /// </summary>
        /// <param name="point1">точка</param>
        /// <param name="point2">точка</param>
        /// <param name="point3">точка</param>
        public Figure(Point point1, Point point2, Point point3)
        {
            Points = new Point[] { point1, point2, point3 };
            Name = "Triangle";
        }
        /// <summary>
        /// констрктор с 4 параметрами - точками
        /// </summary>
        /// <param name="point1">точка</param>
        /// <param name="point2">точка</param>
        /// <param name="point3">точка</param>
        /// <param name="point4">точка</param>
        public Figure(Point point1, Point point2, Point point3, Point point4)
        {
            Points = new Point[] { point1, point2, point3, point4 };
            Name = "Rectangle";
        }

        /// <summary>
        /// констрктор с 5 параметрами - точками
        /// </summary>
        /// <param name="point1">точка</param>
        /// <param name="point2">точка</param>
        /// <param name="point3">точка</param>
        /// <param name="point4">точка</param>
        /// <param name="point5">точка</param>
        public Figure(Point point1, Point point2, Point point3, Point point4, Point point5)
        {
            Points = new Point[] { point1, point2, point3, point4, point5 };
            Name = "Polygon";
        }

        /// <summary>
        /// метод высчитывает и возвращает расстояие между двумя точками
        /// </summary>
        /// <param name="A">первая точка</param>
        /// <param name="B">вторая точка</param>
        /// <returns>расстояние между точками</returns>
        private double LengthSide(Point A, Point B)
        {
            return Math.Sqrt(Math.Pow(A.X - B.X, 2) + Math.Pow(A.Y - B.Y, 2));
        }

        /// <summary>
        /// метод высчитывает периметр фигуры
        /// </summary>
        /// <returns>периметр фигуры</returns>
        public double PerimeterCalculator()
        {
            //переменная накаплимает длины сторон фигуры
            //считает длину стороны между первой и последней точками в массиве
            //присвиваем значение переменной
            double perimeter = LengthSide(Points[0], Points[Points.Length - 1]);

            //перебираем остатьные точки
            for (int i = 0; i < Points.Length - 1; i++)
            {
                perimeter += LengthSide(Points[i], Points[i + 1]);
            }
            return perimeter;
        }
    }
}