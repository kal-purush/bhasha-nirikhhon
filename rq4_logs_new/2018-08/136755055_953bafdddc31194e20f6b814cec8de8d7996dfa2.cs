using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace inheritanceClasses
{
    public class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Can you see this text");

            Triangle triangleIsoceles = new Triangle();
            triangleIsoceles.triBase = 10;
            triangleIsoceles.triHeigh = 10;
            triangleIsoceles.TriangleArea();

            Console.ReadKey();
        }
    }
}