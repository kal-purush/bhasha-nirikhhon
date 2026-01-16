using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace XTI_Core;

public sealed class NumericValueTypeConverter<T> : TypeConverter
    where T : NumericValue
{
    private readonly NumericValueTypeConverter converter;

    public NumericValueTypeConverter()
    {
        converter = new NumericValueTypeConverter(typeof(T));
    }

    public override bool CanConvertFrom(ITypeDescriptorContext? context, Type sourceType) =>
        converter.CanConvertFrom(context, sourceType);

    public override object? ConvertFrom(ITypeDescriptorContext? context, CultureInfo? culture, object value) =>
        converter.ConvertFrom(context, culture, value);

    public override bool CanConvertTo(ITypeDescriptorContext? context, Type? destinationType) =>
        converter.CanConvertTo(context, destinationType);

    public override object? ConvertTo(ITypeDescriptorContext? context, CultureInfo? culture, object? value, Type destinationType) =>
        converter.ConvertTo(context, culture, value, destinationType);
}

public sealed class NumericValueTypeConverter : TypeConverter
{
    private readonly Type typeToConvert;

    public NumericValueTypeConverter(Type typeToConvert)
    {
        this.typeToConvert = typeToConvert;
    }

    public override bool CanConvertFrom(ITypeDescriptorContext? context, Type sourceType) =>
        sourceType == typeof(int) || sourceType == typeof(string) || base.CanConvertFrom(context, sourceType);

    public override object? ConvertFrom(ITypeDescriptorContext? context, CultureInfo? culture, object value)
    {
        object? numericValue;
        if (value is string)
        {
            var valuesField = typeToConvert.GetField("Values", BindingFlags.Static | BindingFlags.Public);
            if (valuesField == null)
            {
                throw new ArgumentNullException("Values field not found");
            }
            var values = valuesField.GetValue(typeToConvert);
            var valueMethod = valuesField.FieldType.GetMethod("Value", new[] { typeof(string) });
            if (valueMethod == null)
            {
                throw new ArgumentNullException("Value method not found");
            }
            numericValue = valueMethod.Invoke(values, new[] { value });
            if (numericValue == null)
            {
                throw new ArgumentNullException("numeric value should not be null");
            }
        }
        else if (value is int)
        {
            var valuesField = typeToConvert.GetField("Values", BindingFlags.Static | BindingFlags.Public);
            if (valuesField == null)
            {
                throw new ArgumentNullException("Values field not found");
            }
            var values = valuesField.GetValue(typeToConvert);
            var valueMethod = valuesField.FieldType.GetMethod("Value", new[] { typeof(int) });
            if (valueMethod == null)
            {
                throw new ArgumentNullException("Value method not found");
            }
            numericValue = valueMethod.Invoke(values, new[] { value });
            if (numericValue == null)
            {
                throw new ArgumentNullException("numeric value should not be null");
            }
        }
        else
        {
            numericValue = base.ConvertFrom(context, culture, value);
        }
        return numericValue;
    }

    public override bool CanConvertTo(ITypeDescriptorContext? context, Type? destinationType) =>
        destinationType == typeof(TextValue);

    public override object? ConvertTo(ITypeDescriptorContext? context, CultureInfo? culture, object? value, Type destinationType)
    {
        if (value is NumericValue textValue)
        {
            if (destinationType == typeof(int))
            {
                return textValue.Value;
            }
            return textValue.DisplayText;
        }
        return base.ConvertTo(context, culture, value, destinationType);
    }
}