using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TasksConsoleApp
{
    class Program
    {
        static void Main(string[] args)
        {

            ClassA ref1 = new ClassA();
            ref1.value = 7;

            ClassA ref2 = new ClassA();
            ref2.value = 7;

            Console.WriteLine(ref1==ref2);
            Console.WriteLine(ref1.Equals(ref2));
            Console.WriteLine(ReferenceEquals(ref1,ref2));
            Console.ReadLine();
        }


       // static void Chfdytybt
    }





    class ClassA
    {
        public override bool Equals(object obj)
        {
            ClassA that = (ClassA)obj;
            return that.value == this.value;
        }
        public int value = 6;
    }
}