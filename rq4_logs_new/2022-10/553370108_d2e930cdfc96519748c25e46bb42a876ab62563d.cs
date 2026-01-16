using System;
using System.Diagnostics;

namespace Restaurants
{
    class Program
    {

        static void Main(string[] args)
        {
            Console.OutputEncoding = System.Text.Encoding.UTF8;
            var rest = new Restaurant();

            TimerCallback tm = new TimerCallback(rest.AutoCleanBookTableAsync);
            // создаем таймер
            Timer timer = new Timer(tm, (int)1, 0, 20000);

            while (true)
            {
                Console.WriteLine("Привет! Желаете забронировать столик?\n1 - мы уведомим Вас по смс (асинхронно)" +
                    "\n2 - подождите на линии, мы Вас оповестим (синхронно)" +
                    "\nЖелаете снять бронь со столика?" +
                    "\n3 - мы уведомим Вас по смс (асинхронно)" +
                    "\n4 - подождите на линии, мы Вас оповестим (синхронно)");
                if (!int.TryParse(Console.ReadLine(), out var choice) && choice is not (1 or 2 or 3 or 4))
                {
                    Console.WriteLine("Введите, пожалуйста, 1, 2, 3 или 4.");
                    continue;
                }

                var stopWatch = new Stopwatch();
                stopWatch.Start();
                switch (choice)
                {
                    case 1:
                        rest.BookFreeTableAsync(1);
                        break;
                    case 2:
                        rest.BookFreeTable(1);
                        break;
                    case 3:
                        rest.FreeBookTableAsync(1);
                        break;
                    case 4:
                        rest.FreeBookTable(1);
                        break;
                    default:
                        Console.WriteLine("Введите, пожалуйста, 1, 2, 3 или 4.");
                        break;
                }
                Console.WriteLine("Спасибо за Ваше обращение!");
                stopWatch.Stop();

                var ts = stopWatch.Elapsed;
                Console.WriteLine($"{ts.Seconds:00}:{ts.Milliseconds:00}");
            }
        }
    }
}