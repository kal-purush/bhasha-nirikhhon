using System;
using System.Collections.Generic;
using System.Reflection;

namespace EasyReflection
{
    public class PropertyInfoAttributePair<TAttribute>
        where TAttribute : Attribute
    {
        public PropertyInfoAttributePair(PropertyInfo propertyInfo, IEnumerable<TAttribute> attributes)
        {
            this.PropertyInfo = propertyInfo;
            this.Attributes = attributes;
        }

        public PropertyInfo PropertyInfo { get; private set; }

        public IEnumerable<TAttribute> Attributes { get; private set; }
    }
}