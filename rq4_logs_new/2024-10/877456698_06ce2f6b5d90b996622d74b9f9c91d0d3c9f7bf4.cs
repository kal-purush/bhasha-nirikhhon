using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Lab5
{
    class Circle : Point
    { //Порожденный класс Окружность 
        protected double radius { get; set; }
        public double Radius
        {
            get { return radius; }
            set { radius = value; }
        }
        public Circle()
        {
            this.Radius = 0;
        }
        public Circle(double x, double y, double radius) : base(x, y)
        {
            this.Radius = radius;
        }
        public override void Print()
        {
            Console.Write($"Окружность:\n");
            Console.Write($"Координаты центра: ({X}, {Y})\n"); Console.Write($"Радиус: {Radius}\n");
        }
        public virtual double Square()
        {
            double square = 3.14 * Math.Pow(Radius, 2); return square;
        }
    }
}