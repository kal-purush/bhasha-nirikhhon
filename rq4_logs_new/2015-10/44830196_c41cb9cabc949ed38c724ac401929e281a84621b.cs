using EasyReflection;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Linq;

namespace System
{
    public static class PropertyInfoAttributePairExtensions
    {
        public static IDictionary<string, IEnumerable<TAttribute>> Keyed<TAttribute>(this IEnumerable<PropertyInfoAttributePair<TAttribute>> propertyInfoAttributePairs)
            where TAttribute : Attribute
        {
            return propertyInfoAttributePairs.Select(pair => new KeyValuePair<string, IEnumerable<TAttribute>>(pair.PropertyInfo.Name, pair.Attributes)).ToDictionary(pair => pair.Key, pair => pair.Value);
        }
    }
}