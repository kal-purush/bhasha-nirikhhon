using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using switchStatement.Parent;

namespace switchStatement.Child
{
    public class Samuel : KevNina // this know as base class
    {
        public string ChildName = "Samuel";
        public int childAge = 10;

        public void PickinName()
        {
            Console.WriteLine("My name is {0}", ChildName);
        }
        public void PickinAge()
        {
            Console.WriteLine("I am {0} years old", childAge );
        }
    }
}