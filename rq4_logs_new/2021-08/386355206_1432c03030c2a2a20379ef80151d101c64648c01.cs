using System;

// Универсальные шаблоны.

namespace Generics_01
{
    // На 9-й строке создаем класс с именем MyClass, параметризированный Указателем Места Заполнения Типом - T

    class MyClass<T>
    {
        public T field;

        public void Method()
        {
            Console.WriteLine(field.GetType());
        }
    }
    
    class MyClass
    {
        public void Method<T>(T argument)
        {
            T variable = argument;

            Console.WriteLine(variable);
        }
    }

    class Program
    {
        static void Main()
        {
            //№1
            
            // Создаем экземпляр класса MyClass и в качестве параметра типа (тип MyClass) передаем тип int.
            MyClass<int> instance1 = new MyClass<int>();
            instance1.Method();

            // Создаем экземпляр класса MyClass и в качестве параметра типа (тип MyClass) передаем тип long.
            MyClass<long> instance2 = new MyClass<long>();
            instance2.Method();

            // Создаем экземпляр класса MyClass и в качестве параметра типа (тип MyClass) передаем тип string.
            MyClass<string> instance3 = new MyClass<string>();
            instance3.field = "ABC";
            instance3.Method();
            
            
            // №2
            MyClass instance = new MyClass();

            instance.Method<string>("Hello world!");

            instance.Method("Привет мир!");

            // Delay.
            Console.ReadKey();
        }
    }
}