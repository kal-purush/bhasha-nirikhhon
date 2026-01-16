using System;
using System.Collections;

// Обработка исключений.

namespace Exceptions_01
{
    class MyClass
    {
        public void MyMethod()
        {
            Exception exception = new Exception("Мое исключение");

            exception.HelpLink = "http://MyCompany.com/ErrorService";
            exception.Data.Add("Причина исключения: ", "Тестовое исключение");
            exception.Data.Add("Время возникновения исключения: ", DateTime.Now);

            throw exception;
        }
    }
    
    // Для создания пользовательского исключения, требуется наследование от System.Exception.
    class UserException : Exception
    {
        public void Method()
        {
            Console.WriteLine("Мое Исключение!");
        }
    }
    class Program
    {
        static void Main()
        {
            // #1
            int a = 1, n = 2;

            try
            {
                // Попытка деления на ноль.
                a = a / (2 - n);

                Console.WriteLine("a = {0}", a);
            }
            catch (Exception e)
            {
                Console.WriteLine("Обработка исключения.");
                Console.WriteLine(e.Message);
            }
            
            // #2
            Exception ex = new Exception("Мое Исключение");

            try
            {
                throw ex;
            }
            catch (Exception e)
            {
                Console.WriteLine("Обработка исключения.");
                Console.WriteLine(e.Message);
            }
            
            // #3
            
            try
            {
                throw new Exception("Мое Исключение");
            }
            catch (Exception e)
            {
                Console.WriteLine("Обработка исключения.");
                Console.WriteLine(e.Message);
            }
            
            // #4
            
            try
            {
                MyClass instance = new MyClass();
                instance.MyMethod();
            }
            catch (Exception e)
            {
                Console.WriteLine("Имя члена:               {0}", e.TargetSite);
                Console.WriteLine("Класс определяющий член: {0}", e.TargetSite.DeclaringType);
                Console.WriteLine("Тип члена:               {0}", e.TargetSite.MemberType);
                Console.WriteLine("Message:                 {0}", e.Message);
                Console.WriteLine("Source:                  {0}", e.Source);
                Console.WriteLine("Help Link:               {0}", e.HelpLink);
                Console.WriteLine("Stack:                   {0}", e.StackTrace);

                foreach (DictionaryEntry de in e.Data)
                    Console.WriteLine("{0} : {1}", de.Key, de.Value);
            }
            
            // #5
            
            try
            {
                throw new UserException();
            }
            catch (UserException e)
            {
                Console.WriteLine("Обработка исключения.");
                e.Method();
            }

            // Delay.
            Console.ReadKey();
            
        }
    }
}