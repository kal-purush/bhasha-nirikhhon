using System;
using Wix.AssemblyInfo;

namespace Sample.TestApp
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            var obj = new WixAssemblyInfoPreprocessorExtension();

            var fileVersionArgs = new[] { @"WixAssemblyInfoExt.dll" };
            var fileVersionCompanyName = obj.EvaluateFunction("fileVersion", "CompanyName", fileVersionArgs);
            Console.WriteLine("FileVersionInfo:");
            Console.WriteLine("CompanyName: {0}", fileVersionCompanyName);

            var assemblyInfoArgs = new[] { @"WixAssemblyInfoExt.dll", "System.Reflection.AssemblyCompanyAttribute" };
            var assemblyCompanyName = obj.EvaluateFunction("assemblyInfo", "Company", assemblyInfoArgs);
            Console.WriteLine("AssemblyInfo:");
            Console.WriteLine("Company: {0}", assemblyCompanyName);
        }
    }
}