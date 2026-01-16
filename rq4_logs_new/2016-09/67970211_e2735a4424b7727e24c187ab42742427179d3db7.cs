using HelloWorldLibrary.Interfaces;
using HelloWorldLibrary.Outputs;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;

namespace HelloWorld.Helpers
{
    public class LoadHelper
    {
        public static IOutput GetOutputType()
        {
            IOutput output = null;
            string outputType = GetAppValue("OutputType");
            string assemblyName = GetAppValue("AssemblyName");

            if (string.IsNullOrWhiteSpace(outputType))
                return new ConsoleOutput(); //Default if not setup correctly.

            if (string.IsNullOrWhiteSpace(assemblyName))
                return new ConsoleOutput(); //Default if not setup correctly.

            System.Runtime.Remoting.ObjectHandle createInstance = Activator.CreateInstance(assemblyName, outputType);
            output = createInstance.Unwrap() as IOutput;
            if (output == null)
                output = new ConsoleOutput(); //Default if not one found.

            return output;
        }

        private static string GetAppValue(string key)
        {
            string[] values = ConfigurationManager.AppSettings.GetValues(key);
            if (values == null || values.Length == 0)
                return null;

            return values[0];
        }
    }
}