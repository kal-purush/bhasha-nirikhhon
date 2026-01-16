using System;
using Collections;
using System.Collections.Generic;

namespace Namespaces
{
    class Program
    {
        static void Main(string[] args)
        {
            //1
            //Add reference to project Collections
            MyDictionary<int, string> countries = new MyDictionary<int, string>();

            countries.Add(1, "Russia");
            countries.Add(3, "Great Britain");
            countries.Add(2, "USA");

            foreach (KeyValuePair<int, string> item in countries)
            {
                Console.Write("{0} - {1}", item.Key, item.Value);
                Console.WriteLine();
            }

            Console.WriteLine();
            //2
            var positions = new MyDictionaryChild<int, string>();

            positions.Add(1, "Trainee");
            positions.Add(3, "Junior");
            positions.Add(2, "Middle");

            Console.WriteLine(positions[3]);
            Console.WriteLine(positions.Count);

            Console.WriteLine();
            //3
            MyNamespace.MyClass myClass = new MyNamespace.MyClass();
            myClass.MyClassMethod();

            Console.ReadLine();
        }
    }
}