using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GBcool
{
    public class MyDrob //класс дробей
    {
        private int x, y; //x числитель, y знаменатель

        public int X //доступ на чтение и запись числителя
        {
            get { return x; }
            set { x = value; }
        }
        public int Y //доступ на чтение и запись знаменателя
        {
            get { return y; }
            set 
            { 
                if (y == 0) { throw new ArgumentException("На ноль делить нельзя"); }
                else y = value; 
            }
        }
        public double Decimal //доступ на чтение десятичной дроби
        {
            get { return (double) x / y; } 
        }

        public MyDrob (int x, int y) //конструктор
        {
            this.x = x;
            if (y == 0)
            {
                throw new ArgumentException("На ноль делить нельзя");
            } else this.y = y;
        }
        public override string ToString()
        {
            return $"{x} / {y}";
        } 

        public static MyDrob Sokratit (MyDrob a) //сокращатель дробей с помощью алгоритма Эвклида
        {

            //int max = 0;
            ////выбираем что больше числитель или знаменатель
            //if (a.x > a.y) max = Math.Abs(a.y);
            //else max = Math.Abs(a.x);

            //for (int i = max; i >= 2; i--)
            //{

            //    if ((a.x % i == 0) & (a.y % i == 0))
            //    {
            //        a.x = a.x / i;
            //        a.y = a.y / i;
            //    }

            //}
            ////передвигаем минус в числитель
            //if (a.y < 0)
            //{
            //    a.x = -1 * a.x;
            //    a.y = Math.Abs(a.y);
            //}
            //return a;
            int nod;
            int m = Math.Abs(a.x), n = Math.Abs(a.y);
            while (m != n)
            {
                if (m > n)
                {
                    m = m - n;
                }
                else
                {
                    n = n - m;
                }
            }

            nod = n; 
            a.x = a.x / nod;
            a.y = a.y / nod;
            if (a.y < 0) //передвигаем минус в числитель для удобства
            {
                a.x = -1 * a.x;
                a.y = Math.Abs(a.y);
            }
            return a;
        }

        public static MyDrob operator + (MyDrob a, MyDrob b) //сложение дробей
        {
            MyDrob DrobDef = new MyDrob((a.x * b.y) + (a.y * b.x) , a.y * b.y);
            return DrobDef;
        }
        public static MyDrob operator - (MyDrob a, MyDrob b) //вычитание дробей
        {
            MyDrob DrobDef = new MyDrob((a.x * b.y) - (a.y * b.x), a.y * b.y);
            return DrobDef;
        }
        public static MyDrob operator * (MyDrob a, MyDrob b) //умножение дробей
        {
            MyDrob DrobDef = new MyDrob(a.x * b.x, a.y * b.y);
            return DrobDef;
        }
        public static MyDrob operator / (MyDrob a, MyDrob b) //деление дробей
        {
            MyDrob DrobDef = new MyDrob(a.x * b.y, a.y * b.x);
            return DrobDef;
        }
    }
        public class ComplexClass
    {
        double re;
        double im;
        

        public ComplexClass()
        {
            re = 0;
            im = 0;
        }
        public ComplexClass(double re, double im)
        {
            this.re = re;
            this.im = im;
        }
        public ComplexClass Plus(ComplexClass x) //сложение комплексных чисел
        {
            ComplexClass y = new ComplexClass(re + x.re, im + x.im);   
            return y;
        }
        public ComplexClass Minus(ComplexClass x) //вычитание комплексных чисел
        {
            ComplexClass y = new ComplexClass(re - x.re, im - x.im);      
            return y;
        }
        public ComplexClass Multi(ComplexClass x) // произведение двух комплексных чисел
        {
            ComplexClass y = new ComplexClass(re * x.re - im * x.im, re * x.im + im * x.re);    
            return y;
        }
        public override string ToString()
        {
            return re + (im > 0 ? "+" : "") + im + "i";
        }
    } //класс для комплексных чисел
    public class Classes
    {
        public struct Complex
        {
            public double im;
            public double re;
            public Complex Plus(Complex x) //сложение комплексных чисел
            {
                Complex y;
                y.im = im + x.im;
                y.re = re + x.re;
                return y;
            }
            public Complex Minus(Complex x) //вычитание комплексных чисел
            {
                Complex y;
                y.im = im - x.im;
                y.re = re - x.re;
                return y;
            }
            public Complex Multi(Complex x) // произведение двух комплексных чисел
            {
                Complex y;
                y.im = re * x.im + im * x.re;
                y.re = re * x.re - im * x.im;
                return y;
            }
            public override string ToString()
            {
                return re + (im>0 ? "+": "") + im + "i";
            }
        }
        public static void Print(string s, int x, int y, ConsoleColor foregroundcolor) //вывод текста по координатам
        {
            if (x != 0 || y != 0) Console.SetCursorPosition(x, y);
            Console.ForegroundColor = foregroundcolor;
            Console.WriteLine(s);
        }
        public static void PrintCenter(string s, ConsoleColor foregroundcolor) //вывод текста по центру строки
        {
            Console.SetCursorPosition((Console.WindowWidth - s.Length) / 2, Console.CursorTop);
            Console.ForegroundColor = foregroundcolor;
            Console.WriteLine(s);
        }
        public static void PrintLeft(string s, bool InLine, ConsoleColor foregroundcolor) //вывод текста по левому краю с отступом
        {
            if (Console.CursorLeft<20) Console.SetCursorPosition(20, Console.CursorTop);
            else Console.SetCursorPosition(Console.CursorLeft, Console.CursorTop);
            Console.ForegroundColor = foregroundcolor;
            if (InLine) Console.Write(s); else Console.WriteLine(s);
        }
        public static void Pause(string s) //пауза
        {
            if (s != "") Console.WriteLine(s);
            Console.ReadKey();
            Console.WriteLine(""); //делаем отступ
        }
        
        public static int NumberSumm(int n) //сумма цифр числа
        {
            int s = 0;
            while (n != 0)
            {
                s = s + n % 10;
                n = n / 10;
            }
            return s;
        }
        public static int NumberCount(int n) //количество цифр в числе
        {
            int s = 0;
            int h = 0;
            while (n != 0)
            {
                s = s + n % 10;
                n = n / 10;
                h++;
            }
            return h;
        }
        public static double IndexMassa(double m, double h) //подсчет индекса массы тела
        {
           return (m / Math.Pow(h/100,2));
        }
        public static void LogoLesson (string s) //шапка программы
        {
            Console.WindowHeight = 45;
            Console.WindowWidth = 130;
            Console.Clear();
            Console.WriteLine("");
            Classes.PrintCenter("░░░░░░▐▀▄░░░░░░░░░▄▀▌░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░", ConsoleColor.Red);
            Classes.PrintCenter("░░░░░░▐▓░▀▄▀▀▀▀▀▄▀░▓▌░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░", ConsoleColor.Red);
            Classes.PrintCenter("░░░░░░▐░▓░▄▀░░░▀▄░▓░▌░░░░▄▀▀ █▀▀ █▀▀ █░█ █▀▄ █▀▄ ▄▀▄ ▀█▀ █▄░█ ▄▀▀░░░", ConsoleColor.Red);
            Classes.PrintCenter("░░░░░░░█░░▌█▐░▌█▐░░█░░░░░█░█ █▀▀ █▀▀ █▀▄ █▀▄ █▀▄ █▄█ ░█░ █▀██ ░▀▄░░░", ConsoleColor.Red);
            Classes.PrintCenter("░░░░▄▄▄▐▀░░░▀█▀░░░▀▌▄▄▄░░░▀▀ ▀▀▀ ▀▀▀ ▀░▀ ▀▀░ ▀░▀ ▀░▀ ▀▀▀ ▀░░▀ ▀▀░░░░", ConsoleColor.Red);
            Classes.PrintCenter("░░░█▐▐▐▌▀▄░▀▄▀▄▀░▄▀▐▌▌▌█░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░", ConsoleColor.Red);
            Console.WriteLine("");
            Classes.PrintCenter("____LESSON_" + s + "________________________________________by_Моисеев_Игорь", ConsoleColor.Yellow);
            Console.WriteLine("");


        }
        public static void RecursiaOne (int a, int b)
        {
            if (a < b)
            {
                PrintLeft(a.ToString()+"\t", true, ConsoleColor.White);
                a++;                
                RecursiaOne(a,b);
            }            
        }//вывод чисел пока a<b
        public static int RecursiaTwo(int a, int b)
        {
            if (a > b) return 0;            
            else
            {
                a++;
                return a - 1 + RecursiaTwo(a, b);
            }
        }//рекурсивный метод, который считает сумму чисел от a до b включительно.

    }//прочее + структрадля комплексных чисел
}