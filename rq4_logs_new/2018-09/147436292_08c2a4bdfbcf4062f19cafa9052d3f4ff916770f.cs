using System;
using System.Collections.Generic;
using System.Text;

namespace legoMiniFigures
{
    interface IGreeter
    {
        string Name { get;  }

        void Greet(IGreetable greetable);
    }

    interface IGreetable
    {
        string Name { get; }
    }

}