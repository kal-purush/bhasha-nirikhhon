using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Nt.Infrastructure.WebApi.ExtensionMethods
{
    public static class TypeExtensions
    {
        public static List<Type> FindAllDerivedTypes(this Type source, Assembly assembly)
        {
            return assembly
                .GetTypes()
                .Where(t =>
                    t != source &&
                    source.IsAssignableFrom(t)
                    ).ToList();

        }
    }
}