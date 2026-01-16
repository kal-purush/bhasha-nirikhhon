namespace GitBitter
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using GitBitterLib;

    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length > 0)
            {
                var unwrapper = new PackageUnwrapper(args[0]);
                unwrapper.StartAndWaitForUnwrapping();
            }
            else
            {
                var unwrapper = new PackageUnwrapper();
                unwrapper.StartAndWaitForUnwrapping();
            }
        }
    }
}