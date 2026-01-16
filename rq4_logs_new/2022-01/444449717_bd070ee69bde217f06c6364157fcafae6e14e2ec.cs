using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Project.ValidationFarmework.anotation;
using Project.ValidationFarmework.helper;

namespace Project.ValidationFarmework.validator
{
    internal class PhoneValidator: Validator
    {
        public PhoneValidator() { }

        protected override string getMessage(PropertyInfo prop)
        {
            Phone attr=(Phone)Attribute.GetCustomAttribute(prop,typeof(Phone));
            return attr.ErrorMessage;
        }

        protected override bool invalid(PropertyInfo prop, object value)
        {
            return !ValidationHelper.IsValidPhone((string)value);
        }
    }
}