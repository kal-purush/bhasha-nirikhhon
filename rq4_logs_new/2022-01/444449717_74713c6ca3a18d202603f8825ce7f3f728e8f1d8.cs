using Project.ValidationFarmework.anotation;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Project.ValidationFarmework.validator
{
    internal class MaxValidator : Validator
    {
        protected override string getMessage(PropertyInfo prop)
        {
            Max attr = (Max)Attribute.GetCustomAttribute(prop, typeof(Max));
            return attr.ErrorMessage;
        }

        protected override bool invalid(PropertyInfo prop, object value)
        {
            Max attr = (Max)Attribute.GetCustomAttribute(prop, typeof(Max));
            return (((string)value).Length > attr.Length);
        }
    }
}