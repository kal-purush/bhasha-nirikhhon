using DesignPatternFour.Structural.Proxy;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DesignPatternFour.Behavioral.Iterator
{
    public static class ClientCodeForUsingIterator
    {
        public static void UseIterator()
        {
            ConcreteCollection collection = new ConcreteCollection();
            collection.AddEmployee(new Employee("Riyad", 100));
            collection.AddEmployee(new Employee("Poly", 101));
            collection.AddEmployee(new Employee("Rajit", 102));
            collection.AddEmployee(new Employee("Moinul", 103));
            collection.AddEmployee(new Employee("Adil", 104));

            Iterator iterator = collection.CreateIterator();

            Console.WriteLine("Iterating over collection:");

            for(Employee emp =iterator.First(); !iterator.IsCompleted; emp=iterator.Next())
            {
                Console.WriteLine($"Id:{emp.ID} & Name: {emp.Name}");
            }
        }
    }
}