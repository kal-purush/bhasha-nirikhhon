using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BMO.GameDevUnity.CSharp1.Pract1
{
    class Program
    {
        static void Main()
        {
            Console.Write("Введите номер задания: ");
            int Choiсe = int.Parse(Console.ReadLine());
            switch (Choiсe)
            {
                case 1:
                    Lab1();
                    break;
                case 2:
                    Lab2();
                    break;
                case 3:
                    Lab3();
                    break;
                case 4:
                    Lab4();
                    break;
                case 5:
                    Lab5();
                    break;
                default:
                    Console.WriteLine("Неверная команда!");
                    Main();
                    break;
            }
        }

        // Первое задание
        static void Lab1()
        {
            Console.Write("Ваше имя: ");
            string FirstName = Console.ReadLine();
            Console.Write("Ваша фамилия: ");
            string LastName = Console.ReadLine();
            Console.WriteLine("Сколько вам полных лет?");
            int Age = int.Parse(Console.ReadLine());
            Console.Write("Ваш рост(округлить до целого): ");
            int Height = int.Parse(Console.ReadLine());
            Console.Write("Ваш вес(округлить до целого): ");
            int Weight = int.Parse(Console.ReadLine());
            string AgeForm;
            if ((Age % 10 == 0 || Age % 10 > 4)||(Age > 10 && Age < 20))
            {
                AgeForm = "лет";
            }
            else if(Age % 10 != 0 && Age % 10 < 2 && (Age > 20 || Age < 10))
            {
                AgeForm = "год";
            }
            else
            {
                AgeForm = "года";
            }
            Console.WriteLine("Вас зовут {0} {1}, вам {2} {3}, ваш рост {4}, вес {5}", 
                FirstName, LastName, Age, AgeForm, Height, Weight);
            Main();
        }

        //Второе задание
        static void Lab2()
        {
            Console.Write("Ваш рост(в сантиметрах): ");
            float Height = float.Parse(Console.ReadLine());
            Height = Height / 100;
            Console.Write("Ваш вес(в килограммах): ");
            float Weight = float.Parse(Console.ReadLine());
            float BMI = Weight / (Height * Height);
            Console.WriteLine("Индекс массы тела = {0:0.##}", BMI);
            Main();
        }

        static void Lab3()
        {
            Console.Write("x1 = ");
            double x1 = double.Parse(Console.ReadLine());
            Console.Write("y1 = ");
            double y1 = double.Parse(Console.ReadLine());
            Console.Write("x2 = ");
            double x2 = double.Parse(Console.ReadLine());
            Console.Write("y2 = ");
            double y2 = double.Parse(Console.ReadLine());
            double length = Math.Sqrt(Math.Pow(x2 - x1, 2) + Math.Pow(y2 - y1, 2));
            Console.WriteLine("Расстояние между этими точками {0:0.##}", length);
            Main();
        }

        static void Lab4()
        {
            int a = 10;
            int b = 20;
            Console.WriteLine("Первоначальные значения: a = {0}, b = {1}", a, b);
            a = a + b;
            b = a - b;
            a = a - b;
            Console.WriteLine("Полученные значения: a = {0}, b = {1}", a, b);
            Main();
        }

        static void WriteLineCentered(string text)
        {
            int width = Console.WindowWidth;
            if (text.Length < width)
            {
                text = text.PadLeft((width - text.Length) / 2 + text.Length, ' ');
            }
            Console.WriteLine(text);
        }

        static void Lab5()
        {
            WriteLineCentered("Беленко Михаил Олегович, город Архангельск");
            Main();
        }
    }
}