using System;
using System.Collections.Generic;
using System.Text;

using static System.Console;

namespace Lessons.Practice
{
    class Practice5_VDV
    {
        public static void Start()
        {
            WriteLine("Введите размеры карты");
            
            int xLim = Convert.ToInt32(ReadLine());
            int yLim = Convert.ToInt32(ReadLine());

            WriteLine($"Карта создана с заданными размерами {xLim} и {yLim}");

            WriteLine("Введите координаты пацанов");

            int x = Convert.ToInt32(ReadLine());
            int y = Convert.ToInt32(ReadLine());

            WriteLine($"Пацаны выброшены по координатам {x} и {y}");

            WriteLine("Выполняем сравнение координат");
            
            if (x < xLim && x > 0 && y < yLim && y > 0)
            {
                WriteLine("Пацаны на месте");

                int xUp = x;
                int yUp = y + 1;

                if (yUp < yLim)
                {
                    WriteLine($"Патроны на месте по {xUp} {yUp}");
                }
                int xDow = x;
                int yDow = y - 1;

                if (yDow > 0)
                {
                    WriteLine($"Патроны на месте по {xDow} {yDow}");
                }

                int xLef = x - 1;
                int yLef = y;

                if (yLef > 0)
                {
                    WriteLine($"Патроны на месте по {xLim} {yLim}");
                }

                int xRt = x + 1;
                int yRt = y;

                if (yRt < yLim)
                {
                    WriteLine($"Патроны на месте по {xRt} {yRt}");
                }
            }    
            else
            {
                WriteLine("Пацаны в жопе");
            }
        }
    }
}