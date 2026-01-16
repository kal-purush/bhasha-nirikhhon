using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DesignPatten.Practice.Structural.Facade
{
    //Facade is a structural design pattern that provides a simplified (but limited) interface to a complex system of classes, library or framework.
    //While Facade decreases the overall complexity of the application, it also helps to move unwanted dependencies to one place.
    public class SubSystem1
    {
        public string Task1()
        {
            return "SubSystem1->Task1\r\n";
        }

        public string TaskA()
        {
            return "SubSystem1->TaskA\r\n";
        }
    }

    public class SubSystem2
    {
        public string Task2()
        {
            return "SubSystem2->Task2\r\n";
        }

        public string TaskB()
        {
            return "SubSystem2->TaskB\r\n";
        }
    }

    public class Facade
    {
        private SubSystem1 sys1;
        private SubSystem2 sys2;

        public Facade(SubSystem1 sys1, SubSystem2 sys2)
        {
            this.sys1 = sys1;
            this.sys2 = sys2;
        }
        public string Task()
        {
            string output = "";
            output += this.sys1.Task1();
            output += this.sys2.Task2();
            output += this.sys1.TaskA();
            output += this.sys2.TaskB();
            return output;
        }
    }

    public class Example1Client
    {
        public static void Test()
        {
            SubSystem1 sys1 = new SubSystem1();
            SubSystem2 sys2 = new SubSystem2();
            Facade facade = new Facade(sys1, sys2);
            Console.Write(facade.Task());
        }
    }
}