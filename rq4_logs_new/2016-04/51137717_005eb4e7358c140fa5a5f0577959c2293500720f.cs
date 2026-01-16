using AppVeyorTest;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WikiUpdater {
    class Program {
        static void Main(string[] args) {
            var documentedClassType = typeof(TestClass);
            File.WriteAllText(args[0],
                String.Join(", ", documentedClassType.GetMethods().Select(m => m.Name)));
        }
    }
}