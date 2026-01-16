using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Composite
{
    public abstract class BaseFile
    {
        public BaseFile(string name)
        {
            this.Name = name;
        }

        public string Name { get; set; }

        public virtual void Display(int depth = 0)
        {
            Console.WriteLine(new string('-', depth) + this.Name);
        }
    }
}