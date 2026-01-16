using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace switchStatement.Parent
{
    public class DadMum
    {
        protected string ParentName = "Babs";

        public void CallParent()
        {
            string newName;
            Console.WriteLine("My parent surname is {0}", ParentName);
           
        }

        protected void NewName()
        {
          Console.WriteLine("My name is  still {0}", ParentName);
         
        }
    }
}