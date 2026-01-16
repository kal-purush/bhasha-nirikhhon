using DesignPatterns.StructuralPatterns.Bridge.Colors;
using DesignPatterns.StructuralPatterns.Bridge.Shapes;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DesignPatterns.StructuralPatterns.Bridge
{
    [ExcludeFromCodeCoverage]
    class Program
    {        
        static void Main(string[] args)
        {
            IPainter painter = new RedColor();
            var retangle = new Retangle(painter);
            retangle.Paint();

            painter = new BlueColor();
            var circle = new Circle(painter);
            circle.Paint();

            Console.ReadKey();
        }
    }
}