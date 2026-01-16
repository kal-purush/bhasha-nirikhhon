using System;

namespace Exercise9
{
    class Program
    {
        static void Main(string[] args)
        {
            Point p1 = new Point(5, 2);
            Point p2 = new Point(-3, 6);
            Console.WriteLine("p1 was: (" + p1.x + ", " + p1.y + ")");
            Console.WriteLine("p2 was: (" + p2.x + ", " + p2.y + ")");
            Point.swapPoints(p1, p2);
            Console.WriteLine("p1 now: (" + p1.x + ", " + p1.y + ")");
            Console.WriteLine("p2 now: (" + p2.x + ", " + p2.y + ")");
            Console.ReadKey();
        }
    }
}