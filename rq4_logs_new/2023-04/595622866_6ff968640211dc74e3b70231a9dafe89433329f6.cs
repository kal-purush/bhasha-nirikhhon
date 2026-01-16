using System;
using System.Collections.Generic;
using Autofac;

namespace FileManager.Dependencies
{
    public static class VMDependencies
    {
        public static Dictionary<string, Type> Views = new Dictionary<string, Type>();
        public static IContainer Container { get; set; }
        public static void ConfigureServices(params Type[] pages)
        {
            foreach (var pageType in pages)
            {
                Views.Add(pageType.Name, pageType);
            }
        }

    }
}