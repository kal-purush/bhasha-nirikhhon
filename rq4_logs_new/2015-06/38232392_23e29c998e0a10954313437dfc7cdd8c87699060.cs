using Antlr4.Runtime;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PythonParser
{
    public class KeyPrinter:Python3BaseListener
    {
        // override default listener behavior
        public override void EnterSingle_input(Python3Parser.Single_inputContext context) 
        {
            Console.WriteLine("Oh, a key!");
        }


        public override void ExitAugassign( Python3Parser.AugassignContext context) 
        {
            Console.WriteLine("ExitAugassign");
        }
    }
}