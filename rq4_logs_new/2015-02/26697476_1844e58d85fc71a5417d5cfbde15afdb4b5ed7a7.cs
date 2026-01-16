using System;
using System.Reflection;

namespace FusionMVVM.Tests.Fakes
{
    public class TestAssembly : Assembly
    {
        public override string FullName
        {
            get { return "Application, Version=1.0.0.0, Culture=neutral, PublicKeyToken=f17657a520109f4e"; }
        }

        public override Module[] GetModules(bool getResourceModules)
        {
            return new Module[0];
        }

        public override Type[] GetTypes()
        {
            return new[] { typeof(string), typeof(int), typeof(long), typeof(double) };
        }
    }
}