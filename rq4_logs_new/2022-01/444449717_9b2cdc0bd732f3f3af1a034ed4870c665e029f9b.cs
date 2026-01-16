using Project.ValidationFarmework.anotation;
using System;
using System.Reflection;
using Range = Project.ValidationFarmework.anotation.Range;

namespace Project.ValidationFarmework.validator
{
    internal class RangeValidator : Validator
    {
        public RangeValidator()
        {

        }

        protected override string getMessage(PropertyInfo prop)
        {
            Range attr = (Range)Attribute.GetCustomAttribute(prop, typeof(Range));
            return attr.ErrorMessage;
        }

        protected override bool invalid(PropertyInfo prop, object value)
        {
            Range attr = (Range)Attribute.GetCustomAttribute(prop, typeof(Range));
            return (((string)value).Length < attr.LengthMin || ((string)value).Length > attr.LengthMax);
        }
    }
}