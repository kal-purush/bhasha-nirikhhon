using System;

public class Class1
{
    // Метод создаёт прямую по координатам двух точек в формате int
    internal int[] createLine()  
    {
        int[] line = new int[2];                

        Console.Write($"Введите значение Х: ");
        int.TryParse(Console.ReadLine(), out int number); 
        line[0] = number;                                   //[0] элемент - координата для построения прямой по Х            

        Console.Write($"Введите значение У: ");
        int.TryParse(Console.ReadLine(), out number);
        line[1] = number;                                   //[1] элемент - координата для построения прямой по У    

        return line;
    }


    // метод проверяет: является ли точка в заданом уравнении точкой пересечения двух линий
    internal void pointOf_intersectionOf_twoLines(int[] FirstLine, int[] SecondLine)  

    {
        //координата Х             координата У
        int k1 = FirstLine[0], b1 = FirstLine[1];
       int k2 = SecondLine[0], b2 = SecondLine[1];

        if (k2 != k1)
        {
            int x = (b1 - b2) / (k2 - k1);
            int y = k2 * x + b2;

            if ((y - k1 * x - b1 == 0) && (y - k2 * x - b2 == 0))
            {
                Console.WriteLine($"Координаты точки пересечения прямых: ({x};{y})");
            }
            else if ((y - k1 * x - b1 == 0) || (y - k2 * x - b2 == 0))
            {
                Console.WriteLine($"Точка лежит на одной из прямых, но не является точкой пересечения");
            }
            else
            {
                Console.WriteLine($"Точка не лежит ни на одной из прямых");
            }

        } else
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"Error. Недопустимые значения \"{k1}\" и \"{k2}\" для данного примера.");
            Console.WriteLine($"Координаты для построения прямых по оси Х не должны быть одинаковыми.");
            Console.ResetColor();
        }

    }     
}