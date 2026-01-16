using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DesignPatterns.OOP.Interface.Files
{
    //Super class or base class
    public abstract class File
    {
        //Encapsulation
        private string _name;

        public string Name
        {
            get { return _name; }
            set { _name = value;  }
        }

        public virtual void Open()
        {
            Console.WriteLine("Opening file...");
        }
    }
}