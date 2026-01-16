using System;
using System.Collections.Generic;

namespace generic
{
    //generic class implementation
    class Generic_test<T>
    {
        T _value;

        public Generic_test(T t)
        {
            this._value = t;
        }

        public void write()
        {
            Console.WriteLine(this._value);
        }
    }
    class Program
    {
        static void Main(string[] args)
        {
            Generic_test<int> test1 = new Generic_test<int>(3);
            Generic_test<string> test2 = new Generic_test<string>("Test");
            test1.write();
            test2.write();
            List<bool> test3 = GetList<bool>(true, 2);
            List<int> test4 = GetList<int>(3, 2);
            foreach (bool value in test3)
            {
                Console.WriteLine(value);
            }
            foreach (int value in test4)
            {
                Console.WriteLine(value);
            }
        }

        //generic function implementation
        static List<T> GetList<T>(T value, int count)
        {
            List<T> list = new List<T>();
            for (int i = 0; i < count; i++)
            {
                list.Add(value);
            }
            return list;
        }
    }
}