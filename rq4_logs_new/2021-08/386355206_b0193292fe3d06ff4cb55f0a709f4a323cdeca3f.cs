using System;
/*
 *  Структуры
 */
namespace Structures
{
    public class ClassPoint // класс "ссылочный" тип
    {
        public int X { get; set; }
        public int Y { get; set; }

        public void Print()
        {
            Console.WriteLine($"X: {X}\tY:{Y}");
        }
    }

    public struct StructPoint // структура "значимый" тип
    {
        public int X { get; set; }
        public int Y { get; set; }

        public void Print()
        {
            Console.WriteLine($"X: {X}\tY:{Y}");
        }
    }
    class Program
    {
        static void Foo(ClassPoint classPoint)
        {
            classPoint.X++;
            classPoint.Y++;
        }

        static void Bar(StructPoint structPoint)
        {
            structPoint.X++;
            structPoint.Y++;
        }
        static void Main(string[] args)
        {
            ClassPoint classPoint = new ClassPoint();

            StructPoint structPoint = new StructPoint(); // new помещает в поля структуры default значения
            
            Foo(classPoint);
            Bar(structPoint);
        }
    }
}