using BH.oM.Base;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace BH.Engine.Test.CodeCompliance.DynamicChecks
{
    public static partial class Query
    {
        public static bool ConvertMethodIsValid(this MethodInfo method)
        {
            if (!method.IsPublic && method.DeclaringType.Name != "Convert") return true;

            string name = method.Name;
            if(Regex.Match(name, "I?To.*").Success)
            {
                var firstparam = method.GetParameters().FirstOrDefault();
                return firstparam != null && typeof(IObject).IsAssignableFrom(firstparam.ParameterType);
            }
            else if(Regex.Match(name, "I?From.*").Success)
            {
                return typeof(IObject).IsAssignableFrom(method.ReturnType);
            }
            return true;
        }
    }
}