using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace FreeView.Scripts.Utils
{
    public static class ReflectionUtils
    {
        public static IEnumerable<T> GetEnumerableOfType<T>(params object[] constructorArgs) where T : class
        {
            var objects = Assembly.GetAssembly(typeof(T))
                .GetTypes()
                .Where(myType => myType.IsClass && !myType.IsAbstract && myType.IsSubclassOf(typeof(T)))
                .Select(type => (T)Activator.CreateInstance(type, constructorArgs))
                .ToList();
            objects.Sort();
            return objects;
        }
    }
}