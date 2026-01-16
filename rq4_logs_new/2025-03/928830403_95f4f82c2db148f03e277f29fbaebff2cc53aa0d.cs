namespace _7_2
//Создайте метод CalcCube для вычисления объема и площади поверхности куба по длине его ребра. Метод должен иметь 3 параметра - длина ребра, out-параметр для объема и out-параметр для площади поверхности.
{
    internal class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Введите длину стороны куба:");
            double edgeLength = Convert.ToDouble(Console.ReadLine());
            double volume;
            double surfaceArea;
            CalcCube(edgeLength, out volume, out surfaceArea);
        }
        static void CalcCube(double a, out double v, out double s)
        {
            v=0; s=0;
            if (a <= 0)
            {
                Console.WriteLine("Куб с такими сторонами не существует");
            }
            else
            {
                v = Math.Pow(a, 3);
                s = Math.Pow(a, 2) * 6;
                Console.WriteLine($"Объем куба: {v}");
                Console.WriteLine($"Площадь поверхности куба: {s}");
            }
        }
    }
}