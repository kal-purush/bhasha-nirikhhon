using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleAppTest
{
    class Program
    {
        static void Main(string[] args)
        {
            BaseClass Obj1 = new BaseClass();
            Child Obj2 = new Child();
            Obj2.Metod1();
            Obj2.Metod2();
            ((BaseClass)Obj2).Metod1();
            ((BaseClass)Obj2).Metod2();
        }
    }


    public class BaseClass
    {
        public virtual void Metod1()
        {
            Console.WriteLine("Base");
        }

        public new void Metod2()
        {
            Console.WriteLine("Base");
        }

    }

    public class Child: BaseClass
    {
        public override void Metod1()
        {
            Console.WriteLine("Child");
        }

        public void Metod2()
        {
            Console.WriteLine("Child");
        }

    }
}