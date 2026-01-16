using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Task1.FormatProvider
{

    /// <summary>
    /// Format provider for numeric values
    /// G (default) - numeric string without formatting
    /// D - whitespace as group separator; coma as fraction separator;
    /// C - coma as group separator; dot as fraction separator;
    /// </summary>
    public class CustomerFormatProvider: IFormatProvider, ICustomFormatter
    {

        public object GetFormat(Type formatType)
        {
            if (formatType == typeof(ICustomFormatter))
                return this;
            else
                return null;
        }

        public string Format(string format, object arg, IFormatProvider formatProvider)
        {
            if (String.IsNullOrEmpty(format))
                format = "G";

            if (arg is decimal)
            {
                decimal num = (decimal)arg;
                var f = new NumberFormatInfo();

                switch (format)
                {
                    case "G":
                        return num.ToString();
                    case "D":
                        f.NumberGroupSeparator = " ";
                        return num.ToString("n", f);
                    case "C":
                        f.NumberGroupSeparator = ",";
                        return num.ToString("n", f);
                    default:
                        throw new FormatException();
                }
            }
            else
            {
                try
                {
                    return HandleOtherFormats(format, arg);
                }
                catch(FormatException)
                {
                    throw new FormatException();
                }
            }
                
        }

        private string HandleOtherFormats(string format, object arg)
        {
            if (arg is IFormattable)
                return ((IFormattable)arg).ToString(format, CultureInfo.CurrentCulture);
            else if (arg != null)
                return arg.ToString();
            else
                return String.Empty;
        }
    }
}